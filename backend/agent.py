import warnings
warnings.filterwarnings("ignore")

import os
import sys
import json
import logging
import tiktoken
from typing import TypedDict, Annotated, List, Optional
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI, AzureChatOpenAI
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage, AIMessage, ToolMessage
from langgraph.graph import StateGraph, END, START
from langgraph.prebuilt import ToolNode
from langgraph.graph.message import add_messages
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from langchain_mcp_adapters.tools import load_mcp_tools
from langchain_groq import ChatGroq

env_file = os.path.join(os.path.dirname(__file__), "env")
load_dotenv(env_file, override=True)

# ─────────────────────────────────────────────────────────────
# Logging Setup
# ─────────────────────────────────────────────────────────────
log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "agent.log")

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.INFO)

file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(console_handler)
logger.addHandler(file_handler)

logger.info(f"\n{'='*80}\nLogging initialized. Logs saved to: {log_file}\n{'='*80}\n")

# ─────────────────────────────────────────────────────────────
# Token Utilities
# ─────────────────────────────────────────────────────────────
try:
    token_encoder = tiktoken.get_encoding("cl100k_base")
except Exception:
    token_encoder = None

def estimate_tokens(text: str) -> int:
    """Estimate token count for text."""
    if token_encoder:
        return len(token_encoder.encode(text))
    return len(text) // 4


# ─────────────────────────────────────────────────────────────
# Agent State
# ─────────────────────────────────────────────────────────────
class AgentState(TypedDict):
    """Full state carried through all graph phases."""
    messages: Annotated[List[BaseMessage], add_messages]

    # Phase 1 output: raw DB schema JSON
    db_schemas: Optional[str]

    # Phase 2 output: structured LLM execution plan
    execution_plan: Optional[List[dict]]   # [{db, tool, reason, query_hint}]

    # Phase tracker
    phase: Optional[str]   # "schema_fetch" | "planning" | "execute_first" | "continue"

    # Collected results per DB (for future multi-query chaining)
    query_results: Optional[List[dict]]


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────
def _build_llm(tools=None):
    """Build LLM from env config, optionally bound to tools."""
    provider = os.getenv("MODEL_PROVIDER", "groq").lower()
    model_name = os.getenv("MODEL_NAME", "llama-3.1-8b-instant")

    if provider == "groq":
        llm = ChatGroq(
            api_key=os.getenv("GROQ_API_KEY"),
            model=model_name,
            temperature=0,
            streaming=True
        )
    elif provider == "azure":
        llm = AzureChatOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            azure_deployment=model_name,
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview"),
            temperature=0,
            streaming=True
        )
    else:
        llm = ChatOpenAI(
            api_key=os.getenv("OPENROUTER_API_KEY"),
            base_url=os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"),
            model=model_name,
            temperature=0,
            streaming=True,
            default_headers={
                "HTTP-Referer": "https://localhost:3000",
                "X-Title": "Multi-DB AI Chat (LangGraph)"
            }
        )

    return llm.bind_tools(tools) if tools else llm


def _truncate_messages(messages, max_tpm):
    """Trim message history to stay within token budget."""
    safe_threshold = int(max_tpm * 0.95)
    total_tokens = sum(estimate_tokens(str(m.content)) for m in messages)

    if total_tokens <= safe_threshold:
        return messages

    logger.warning(f"Token budget exceeded ({total_tokens} > {safe_threshold}). Truncating history...")
    system_msgs = [m for m in messages if isinstance(m, SystemMessage)]
    other_msgs  = [m for m in messages if not isinstance(m, SystemMessage)]

    reduced = system_msgs.copy()
    if other_msgs and isinstance(other_msgs[0], HumanMessage):
        reduced.append(other_msgs[0])
        other_msgs = other_msgs[1:]
    reduced.extend(other_msgs[-2:] if len(other_msgs) > 2 else other_msgs)

    # Aggressive truncation of last message if still over budget
    total_tokens = sum(estimate_tokens(str(m.content)) for m in reduced)
    if total_tokens > safe_threshold and reduced:
        last = reduced[-1]
        other_tokens = sum(estimate_tokens(str(m.content)) for m in reduced[:-1])
        allowed_chars = int(max(1000, safe_threshold - other_tokens) * 3.5)
        content_str = str(last.content)
        if len(content_str) > allowed_chars:
            last.content = content_str[:allowed_chars] + "\n\n...[TRUNCATED DUE TO TOKEN LIMIT]..."

    logger.info(f"Original msgs: {len(messages)} → Reduced: {len(reduced)}")
    return reduced


# ─────────────────────────────────────────────────────────────
# Graph Nodes
# ─────────────────────────────────────────────────────────────

def _log_section(title: str):
    logger.info("\n" + "="*80)
    logger.info(title)
    logger.info("="*80)


async def node_fetch_schemas(state: AgentState, tool_node: ToolNode, llm_with_tools) -> dict:
    """
    PHASE 1 — Fetch all DB schemas via the get_database_info MCP tool.
    Injects a forced tool call to guarantee schema is loaded before planning.
    """
    _log_section("PHASE 1: FETCHING ALL DATABASE SCHEMAS")

    # Force a tool call to get_database_info
    forced_call = AIMessage(
        content="",
        tool_calls=[{
            "id": "schema_fetch_001",
            "name": "get_database_info",
            "args": {}
        }]
    )

    # Execute the tool via ToolNode
    tool_result_state = await tool_node.ainvoke({"messages": [forced_call]})
    tool_messages = tool_result_state.get("messages", [])

    schema_json = None
    for msg in tool_messages:
        if isinstance(msg, ToolMessage) and msg.tool_call_id == "schema_fetch_001":
            schema_json = msg.content
            break

    if schema_json:
        _log_section("DB SCHEMAS FETCHED SUCCESSFULLY")
        logger.info(schema_json[:10000])  # Log first 2000 chars
    else:
        logger.error("Failed to fetch DB schemas — tool returned no result.")
        schema_json = "{}"

    return {
        "db_schemas": schema_json,
        "phase": "planning",
        "messages": tool_messages  # append schema tool messages to state
    }


async def node_plan_execution(state: AgentState, llm_no_tools) -> dict:
    """
    PHASE 2 — Ask the LLM (without tool binding) to produce a structured
    execution plan: an ordered list of databases/tools to query, with reasons.
    Returns JSON plan embedded in the message stream.
    """
    _log_section("PHASE 2: LLM GENERATING EXECUTION PLAN")

    user_query = ""
    for m in state["messages"]:
        if isinstance(m, HumanMessage):
            user_query = m.content
            break

    db_schemas = state.get("db_schemas", "{}")

    planning_prompt = f"""
You are a senior data engineer working with a multi-database retail system.
The system has these databases:

{db_schemas}

The user asked: "{user_query}"

Your task is to produce a precise EXECUTION PLAN as a JSON array.
Each step must have:
  - "step": (integer, 1-based order)
  - "db": (one of "InventoryDB_SQL_Server", "SalesDB_PostgreSQL", "CustomerDB_MongoDB")
  - "tool": (one of "query_inventory_db", "query_sales_db", "query_customer_db")
  - "reason": (short explanation of why this DB is queried at this step)
  - "query_hint": (the actual SQL/NoSQL query to execute)

Rules:
1. Order the steps to minimize cross-joins. Note: For "Top" queries, the database containing the ranking metric (e.g., order amount in SalesDB) is the primary starting point, even if the user asks for "Customers" or "Products".
2. If a later step depends on IDs from an earlier step, say so in the "reason".
3. Output ONLY valid JSON. No markdown, no explanation outside the array.
4. Write SIMPLE, direct SQL/NoSQL queries. Do NOT generate unnecessary nested subqueries. Use basic JOINs or simple SELECTs with LIMIT for straightforward requests.
5. IMPORTANT: Quote reserved keywords appropriately for the target DB (e.g., "Order" in PostgreSQL, [Order] in SQL Server).
6. CRITICAL: If the user's request can be entirely fulfilled by querying a SINGLE database (e.g., getting top orders from SalesDB), your execution plan MUST contain ONLY ONE step.
7. "Top" vs "Latest" Definitions:
   - "Top" means based on Ranking / value, sorted by a metric (e.g., score, amount, revenue), highest sales, or best results.
   - "Latest" means based on Time, sorted by date/time (DESC), recent entries, or newest data.
8. Specific Multi-Step Logic for "Top" entities:
   - For "Top N Customers":
       Step 1: Query SalesDB_PostgreSQL to identify top N customers based on order amount (e.g., SELECT customer_id, order_amount FROM "Order" ORDER BY order_amount DESC LIMIT N).
       Step 2: Use the IDs from Step 1 to fetch full customer profile details from CustomerDB_MongoDB.
       Synthesis: Combine customer details (name, profile) with the order amount in the final table.
   - For "Top N Products":
       Step 1: Query SalesDB_PostgreSQL to identify top products by order amount.
       Step 2: Use product IDs to fetch product names and details from InventoryDB_SQL_Server.

Example format:
[
  {{
    "step": 1,
    "db": "SalesDB_PostgreSQL",
    "tool": "query_sales_db",
    "reason": "Identify top 2 customers by order amount first.",
    "query_hint": "SELECT customer_id, order_amount FROM \\"Order\\" ORDER BY order_amount DESC LIMIT 2"
  }},
  {{
    "step": 2,
    "db": "CustomerDB_MongoDB",
    "tool": "query_customer_db",
    "reason": "Fetch profile details for the identified customer IDs.",
    "query_hint": "{{\\"customer_id\\": {{\\"$in\\": [92, 28]}}}}"
  }}
]
""".strip()

    messages = [
        SystemMessage(content="You are a precise query planner. Respond ONLY with a valid JSON array."),
        HumanMessage(content=planning_prompt)
    ]

    _log_section("PLANNING PROMPT SENT TO LLM")
    logger.info(planning_prompt[:1500])

    response = await llm_no_tools.ainvoke(messages)
    plan_text = response.content.strip()

    logger.info("\nLLM EXECUTION PLAN RESPONSE:")
    logger.info(plan_text)

    # Parse the plan JSON
    execution_plan = []
    try:
        # Strip markdown code fences if present
        if plan_text.startswith("```"):
            plan_text = "\n".join(plan_text.split("\n")[1:])
        if plan_text.endswith("```"):
            plan_text = "\n".join(plan_text.split("\n")[:-1])
        execution_plan = json.loads(plan_text.strip())
        _log_section("EXECUTION PLAN PARSED SUCCESSFULLY")
        for step in execution_plan:
            logger.info(f"  Step {step['step']}: [{step['db']}] → {step['tool']}")
            logger.info(f"    Reason: {step['reason']}")
            logger.info(f"    Query:  {step['query_hint']}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse execution plan JSON: {e}\nRaw: {plan_text}")
        execution_plan = []

    return {
        "execution_plan": execution_plan,
        "phase": "execute_first",
        "messages": [AIMessage(content=f"**Execution Plan:**\n```json\n{json.dumps(execution_plan, indent=2)}\n```")]
    }


async def node_execute_first_query(state: AgentState, tool_node: ToolNode) -> dict:
    """
    PHASE 3 — Execute only the FIRST step of the execution plan.
    Streams the result back and stores it. The main agent loop can then
    continue with remaining steps if needed.
    """
    _log_section("PHASE 3: EXECUTING FIRST QUERY")

    execution_plan: List[dict] = state.get("execution_plan", [])
    if not execution_plan:
        logger.error("No execution plan found. Cannot execute first query.")
        return {
            "phase": "continue",
            "query_results": [],
            "messages": [AIMessage(content="⚠️ No execution plan was generated. Please rephrase your question.")]
        }

    first_step = execution_plan[0]
    tool_name = first_step.get("tool", "")
    query_hint = first_step.get("query_hint", "")
    db_name = first_step.get("db", "")
    reason = first_step.get("reason", "")

    logger.info(f"\n>>> EXECUTING STEP 1 OF {len(execution_plan)}")
    logger.info(f"    DB:    {db_name}")
    logger.info(f"    Tool:  {tool_name}")
    logger.info(f"    Query: {query_hint}")
    logger.info(f"    Why:   {reason}")

    # Build the tool call based on which tool it is
    tool_call_id = "first_query_001"

    if tool_name == "query_customer_db":
        # MongoDB tool signature: collection_name, query_payload, query_type
        try:
            payload_obj = json.loads(query_hint) if isinstance(query_hint, str) else query_hint
        except Exception:
            payload_obj = {}
        tool_args = {
            "collection_name": "customer",
            "query_payload": json.dumps(payload_obj),
            "query_type": "find"
        }
    elif tool_name in ("query_inventory_db", "query_sales_db"):
        tool_args = {"sql_query": query_hint}
    else:
        logger.error(f"Unknown tool: {tool_name}")
        return {
            "phase": "continue",
            "query_results": [],
            "messages": [AIMessage(content=f"⚠️ Unknown tool '{tool_name}' in execution plan.")]
        }

    forced_call = AIMessage(
        content="",
        tool_calls=[{
            "id": tool_call_id,
            "name": tool_name,
            "args": tool_args
        }]
    )

    print(f"\n\033[96m{'='*80}\nFIRST QUERY ({tool_name})\n{'-'*80}\n{query_hint}\n{'='*80}\033[0m\n")

    tool_result_state = await tool_node.ainvoke({"messages": [forced_call]})
    tool_messages = tool_result_state.get("messages", [])

    result_content = None
    for msg in tool_messages:
        if isinstance(msg, ToolMessage) and msg.tool_call_id == tool_call_id:
            result_content = msg.content
            break

    parsed_results = []
    if result_content:
        try:
            parsed_results = json.loads(result_content)
        except Exception:
            parsed_results = [{"raw": result_content}]
        logger.info(f"\nFIRST QUERY RETURNED {len(parsed_results)} record(s).")
    else:
        logger.warning("First query returned no results.")

    query_results = [{
        "step": 1,
        "db": db_name,
        "tool": tool_name,
        "query": query_hint,
        "records": parsed_results
    }]

    summary_msg = (
        f"✅ **Step 1 complete** — Queried `{db_name}` via `{tool_name}`\n"
        f"- **Reason:** {reason}\n"
        f"- **Records returned:** {len(parsed_results)}\n\n"
        f"Proceeding with remaining steps..." if len(execution_plan) > 1
        else f"✅ **Step 1 complete** — Queried `{db_name}` via `{tool_name}`\n"
             f"- **Records returned:** {len(parsed_results)}"
    )

    return {
        "phase": "continue",
        "query_results": query_results,
        "messages": tool_messages + [AIMessage(content=summary_msg)]
    }


async def node_call_model(state: AgentState, llm_with_tools) -> dict:
    """
    PHASE 4+ — Standard ReAct agent loop for remaining steps.
    Executes any remaining queries from the plan and synthesizes the final answer.
    """
    _log_section("PHASE 4+: MAIN AGENT LOOP (REMAINING STEPS / SYNTHESIZE)")

    messages = state["messages"]
    max_tpm = int(os.getenv("MAX_TOKEN_LIMIT", "128000"))
    messages = _truncate_messages(messages, max_tpm)

    # Append execution plan context for remaining work
    execution_plan = state.get("execution_plan", [])
    query_results = state.get("query_results", [])
    completed_steps = {r["step"] for r in query_results}
    remaining_steps = [s for s in execution_plan if s["step"] not in completed_steps]

    if remaining_steps:
        context = (
            f"\n\nEXECUTION PLAN PROGRESS:\n"
            f"- Completed: {list(completed_steps)}\n"
            f"- Remaining: {json.dumps(remaining_steps, indent=2)}\n"
            f"- Results so far: {json.dumps(query_results, indent=2, default=str)[:2000]}\n\n"
            f"Now execute the remaining queries in order, then synthesize a final Markdown answer."
        )
    else:
        context = (
            f"\n\nEXECUTION PLAN PROGRESS:\n"
            f"- All steps are complete.\n"
            f"- Results so far: {json.dumps(query_results, indent=2, default=str)[:3000]}\n\n"
            f"Please synthesize these results into a clean Markdown TABLE to answer the user's question."
        )
        
    messages = messages + [SystemMessage(content=context)]

    logger.info("SENDING FINAL MESSAGES TO LLM:")
    for i, msg in enumerate(messages):
        logger.info(f"  [{i}] {type(msg).__name__}: {str(msg.content)[:200]}")

    response = await llm_with_tools.ainvoke(messages)

    if hasattr(response, 'tool_calls') and response.tool_calls:
        _log_section("LLM DECIDED TO USE TOOLS")
        for tc in response.tool_calls:
            logger.info(f"  - {tc['name']}({tc['args']})")

    if hasattr(response, 'response_metadata') and response.response_metadata:
        usage = response.response_metadata.get('usage', {})
        logger.info(f"\nTokens — Input: {usage.get('input_tokens',0)} | Output: {usage.get('output_tokens',0)}")

    return {"messages": [response]}


def should_continue(state: AgentState):
    """Conditional edge: continue tool loop or end."""
    last_message = state['messages'][-1]
    if not last_message.tool_calls:
        return END
    return "tools"


# ─────────────────────────────────────────────────────────────
# Main Entry Point
# ─────────────────────────────────────────────────────────────
async def run_agent(user_input: str):
    """
    3-Phase LangGraph agent:
      Phase 1 → Fetch all DB schemas via get_database_info
      Phase 2 → LLM produces ordered execution plan (JSON)
      Phase 3 → Execute first query from the plan
      Phase 4+ → Standard ReAct loop for remaining queries + synthesis
    """
    try:
        server_script = os.path.join(os.path.dirname(__file__), "mcp_server.py")
        server_params = StdioServerParameters(
            command=sys.executable,
            args=[server_script],
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                tools = await load_mcp_tools(session)
                tool_node = ToolNode(tools)

                llm_with_tools = _build_llm(tools)
                llm_no_tools   = _build_llm()   # plain LLM for planning (no tool noise)

                # ── Build the full LangGraph ──────────────────────────────
                # We embed closures so nodes capture the LLM/tool_node refs
                async def _schema_node(state):
                    return await node_fetch_schemas(state, tool_node, llm_with_tools)

                async def _plan_node(state):
                    return await node_plan_execution(state, llm_no_tools)

                async def _first_query_node(state):
                    return await node_execute_first_query(state, tool_node)

                async def _main_agent_node(state):
                    return await node_call_model(state, llm_with_tools)

                workflow = StateGraph(AgentState)
                workflow.add_node("fetch_schemas",    _schema_node)
                workflow.add_node("plan_execution",   _plan_node)
                workflow.add_node("execute_first",    _first_query_node)
                workflow.add_node("agent",            _main_agent_node)
                workflow.add_node("tools",            tool_node)

                # Deterministic phases 1→2→3→4
                workflow.add_edge(START,            "fetch_schemas")
                workflow.add_edge("fetch_schemas",  "plan_execution")
                workflow.add_edge("plan_execution", "execute_first")
                workflow.add_edge("execute_first",  "agent")

                # ReAct loop for remaining steps
                workflow.add_conditional_edges("agent", should_continue, {"tools": "tools", END: END})
                workflow.add_edge("tools", "agent")

                app = workflow.compile()

                # System prompt for the final synthesis phase
                system_prompt = (
                    "You are a powerful multi-database retail assistant with access to:\n"
                    "  • SQL Server  — InventoryDB  (products & stock)\n"
                    "  • PostgreSQL  — SalesDB      (orders & revenue)\n"
                    "  • MongoDB     — CustomerDB   (customer profiles)\n\n"
                    "DEFINITIONS:\n"
                    "  - 'Top' means based on Ranking/value, sorted by metric (score, amount, revenue), highest sales, or best results.\n"
                    "  - 'Latest' means based on Time, sorted by date/time (DESC), recent entries, or newest data.\n\n"
                    "IMPORTANT: DB schemas have already been fetched and an execution plan has been generated.\n"
                    "Your job now is to execute any remaining queries from the plan (in order), "
                    "then synthesize all results into a clean Markdown TABLE as the final answer.\n"
                    "SYNTHESIS RULE: For 'Top N' queries across databases, you MUST merge the records (e.g., show Customer Name from MongoDB alongside Order Amount from Postgres).\n"
                    "Handle reserved keywords: [SQL Server] brackets, \"PostgreSQL\" double-quotes."
                )

                initial_state = {
                    "messages": [
                        SystemMessage(content=system_prompt),
                        HumanMessage(content=user_input)
                    ],
                    "db_schemas": None,
                    "execution_plan": None,
                    "phase": "schema_fetch",
                    "query_results": []
                }

                # ── Stream events ─────────────────────────────────────────
                async for event in app.astream_events(initial_state, version="v2"):
                    kind = event["event"]

                    if kind == "on_chat_model_stream":
                        content = event["data"]["chunk"].content
                        if content:
                            yield {"type": "token", "content": content}

                    elif kind == "on_tool_start":
                        tool_name  = event["name"]
                        tool_input = event["data"].get("input")
                        _log_section(f"TOOL CALL: {tool_name}")
                        logger.info(f"Input: {json.dumps(tool_input, indent=2, default=str)}")

                        actual_query = None
                        if isinstance(tool_input, dict):
                            actual_query = tool_input.get("sql_query") or tool_input.get("query_payload")
                        if actual_query:
                            header = f"ACTUAL QUERY ({tool_name})"
                            print(f"\n\033[96m{'='*80}\n{header}\n{'-'*80}\n{actual_query}\n{'='*80}\033[0m\n")
                            logger.info(f"{header}:\n{actual_query}")

                        yield {
                            "type": "tool_start",
                            "tool": tool_name,
                            "input": tool_input
                        }

                    elif kind == "on_tool_end":
                        yield {
                            "type": "tool_end",
                            "tool": event["name"],
                            "status": "success"
                        }

                    elif kind == "on_chain_start":
                        node = event.get("name", "")
                        phase_map = {
                            "fetch_schemas":  "📡 Phase 1: Fetching all database schemas...",
                            "plan_execution": "🧠 Phase 2: LLM is planning query execution order...",
                            "execute_first":  "🚀 Phase 3: Executing first query from the plan...",
                            "agent":          "⚙️  Phase 4+: Running remaining queries & synthesizing..."
                        }
                        if node in phase_map:
                            yield {"type": "phase", "content": phase_map[node]}

    except Exception as e:
        import traceback
        traceback.print_exc()
        yield {"type": "error", "content": str(e)}
