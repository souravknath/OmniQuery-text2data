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
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langgraph.graph import StateGraph, END, START
from langgraph.prebuilt import ToolNode
from langgraph.graph.message import add_messages
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from langchain_mcp_adapters.tools import load_mcp_tools
from langchain_groq import ChatGroq
from schema_analyzer import schema_analyzer
from query_generator import query_generator

load_dotenv(override=True)

# Configure logging - both console and file
log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "agent.log")

# Create formatter
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.INFO)

# File handler
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

# Root logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(console_handler)
logger.addHandler(file_handler)

logger.info(f"\n{'='*80}\nLogging initialized. Logs saved to: {log_file}\n{'='*80}\n")

# Token counter (using GPT tokenizer as approximation)
try:
    token_encoder = tiktoken.get_encoding("cl100k_base")
except Exception:
    token_encoder = None

def estimate_tokens(text: str) -> int:
    """Estimate token count for text."""
    if token_encoder:
        return len(token_encoder.encode(text))
    # Fallback: rough estimate ~4 chars per token
    return len(text) // 4

# --- 1. Define State & Graph Nodes ---
class AgentState(TypedDict):
    """The state of the agent is represented as a list of messages."""
    messages: Annotated[List[BaseMessage], add_messages]

def should_continue(state: AgentState):
    """Conditional edge to decide whether to continue or stop."""
    last_message = state['messages'][-1]
    if not last_message.tool_calls:
        return END
    return "tools"

# --- 2. Main entry point (Async Generator for Streaming) ---
async def run_agent(user_input: str):
    """
    Invokes the LangGraph execution flow and yields chunks of the response.
    """
    try:
        # Setup MCP Client with absolute path to ensure it always finds mcp_server.py
        server_script = os.path.join(os.path.dirname(__file__), "mcp_server.py")
        server_params = StdioServerParameters(
            command=sys.executable,
            args=[server_script],
        )
        
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                
                # Fetch tools dynamically
                tools = await load_mcp_tools(session)
                tool_node = ToolNode(tools)
                
                # Configure LLM based on provider
                provider = os.getenv("MODEL_PROVIDER", "groq").lower()
                model_name = os.getenv("MODEL_NAME", "openai/gpt-oss-120b")
                
                if provider == "groq":
                    llm = ChatGroq(
                        api_key=os.getenv("GROQ_API_KEY"),
                        model=model_name,
                        temperature=0,
                        streaming=True
                    ).bind_tools(tools)
                elif provider == "azure":
                    llm = AzureChatOpenAI(
                        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
                        azure_deployment=model_name,
                        api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview"),
                        temperature=0,
                        streaming=True
                    ).bind_tools(tools)
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
                    ).bind_tools(tools)
                
                # Cache schema once on first query
                schema_cached = False
                
                async def auto_generate_query_if_possible(user_message: str) -> Optional[str]:
                    """
                    Try to auto-generate a SQL query from user intent using metadata.
                    Returns the query if successful, None otherwise (fallback to LLM).
                    """
                    nonlocal schema_cached
                    
                    try:
                        # Cache schema once
                        if not schema_cached:
                            logger.info("\n" + "="*80)
                            logger.info("CACHING DATABASE SCHEMAS")
                            logger.info("="*80)
                            
                            # Get database info once and cache it
                            get_db_info_tool = next((t for t in tools if t.name == 'get_database_info'), None)
                            if get_db_info_tool:
                                schema_str = await get_db_info_tool.func()
                                parsed_schema = query_generator.analyzer.parse_schema_info(schema_str)
                                
                                # Cache for each database
                                for db_name, db_info in parsed_schema.items():
                                    schema_analyzer.cache.set_schema(db_name, db_info)
                                
                                logger.info(f"Cached schemas for: {list(parsed_schema.keys())}\n")
                            schema_cached = True
                        
                        # Parse user intent and generate query
                        logger.info("="*80)
                        logger.info("AUTO-GENERATING QUERY FROM METADATA")
                        logger.info("="*80)
                        
                        intent = query_generator.parse_user_intent(user_message)
                        query = query_generator.generate_from_intent(intent)
                        
                        if query:
                            logger.info(f"✅ Successfully auto-generated query (0 LLM tokens used)\n")
                            return query
                        else:
                            logger.info(f"❌ Could not auto-generate query, falling back to LLM\n")
                            return None
                            
                    except Exception as e:
                        logger.warning(f"Error in auto-generate: {e}, falling back to LLM")
                        return None
                
                async def call_model(state: AgentState):
                    """Main reasoning node."""
                    messages = state['messages']
                    
                    # Estimate token count and truncate if needed
                    total_tokens = 0
                    for msg in messages:
                        content = msg.content if isinstance(msg.content, str) else str(msg.content)
                        total_tokens += estimate_tokens(content)
                    
                    max_tpm = 8000  # Groq on-demand limit
                    safe_threshold = int(max_tpm * 0.95)  # Use 95% to be safe
                    
                    logger.info("\n" + "="*80)
                    logger.info(f"TOKEN COUNT PRE-FLIGHT CHECK")
                    logger.info("="*80)
                    logger.info(f"Estimated total tokens: {total_tokens}")
                    logger.info(f"Rate limit (TPM): {max_tpm}")
                    logger.info(f"Safe threshold (95%): {safe_threshold}")
                    
                    # If exceeding limit, keep only system prompt + last 2 messages
                    if total_tokens > safe_threshold:
                        logger.warning(f"\n⚠️  TOKEN COUNT EXCEEDS SAFE THRESHOLD!")
                        logger.warning(f"Reducing message history to prevent rate limit...\n")
                        
                        # Find system prompt (usually first)
                        system_msgs = [m for m in messages if isinstance(m, SystemMessage)]
                        other_msgs = [m for m in messages if not isinstance(m, SystemMessage)]
                        
                        # Keep system + last 2 messages max
                        reduced_messages = system_msgs + other_msgs[-2:]
                        logger.info(f"Original messages: {len(messages)}, Reduced to: {len(reduced_messages)}")
                        messages = reduced_messages
                        
                        # Recalculate
                        total_tokens = 0
                        for msg in messages:
                            content = msg.content if isinstance(msg.content, str) else str(msg.content)
                            total_tokens += estimate_tokens(content)
                        logger.info(f"Adjusted token count: {total_tokens}\n")
                    
                    # Try to auto-generate query from user intent (BEFORE LLM)
                    user_input = None
                    for msg in messages:
                        if isinstance(msg, HumanMessage):
                            user_input = msg.content if isinstance(msg.content, str) else str(msg.content)
                            break
                    
                    auto_generated_query = None
                    if user_input:
                        auto_generated_query = await auto_generate_query_if_possible(user_input)
                        
                        if auto_generated_query:
                            # Add auto-generated query to the message context
                            auto_gen_msg = SystemMessage(
                                content=f"✅ Auto-Generated Query (0 LLM tokens used):\n\n```sql\n{auto_generated_query}\n```\n\nPlease execute this query and return the results."
                            )
                            messages.append(auto_gen_msg)
                    
                    logger.info("="*80)
                    logger.info("SENDING PROMPT TO LLM")
                    logger.info("="*80)
                    for i, msg in enumerate(messages):
                        msg_type = type(msg).__name__
                        content = msg.content if isinstance(msg.content, str) else str(msg.content)
                        logger.info(f"\n>>> MESSAGE {i} ({msg_type}):")
                        logger.info(content)
                    logger.info("\n" + "="*80 + "\n")
                    
                    response = await llm.ainvoke(messages)
                    
                    # Log token usage
                    if hasattr(response, 'response_metadata') and response.response_metadata:
                        usage = response.response_metadata.get('usage', {})
                        input_tokens = usage.get('input_tokens', 0)
                        output_tokens = usage.get('output_tokens', 0)
                        total_tokens = input_tokens + output_tokens
                        logger.info("\n" + "="*80)
                        logger.info("TOKEN CONSUMPTION")
                        logger.info("="*80)
                        logger.info(f"Input Tokens:  {input_tokens}")
                        logger.info(f"Output Tokens: {output_tokens}")
                        logger.info(f"Total Tokens:  {total_tokens}")
                        logger.info("="*80 + "\n")
                    
                    return {"messages": [response]}
                
                # Build the Graph
                workflow = StateGraph(AgentState)
                workflow.add_node("agent", call_model)
                workflow.add_node("tools", tool_node)

                workflow.add_edge(START, "agent")
                workflow.add_conditional_edges("agent", should_continue, {"tools": "tools", END: END})
                workflow.add_edge("tools", "agent")
                
                # Compile dynamically
                app = workflow.compile()
                
                # Execute graph with streaming events
                system_prompt = (
                    "You are a powerful multi-database retail assistant. You have access to SQL Server (Inventory), "
                    "PostgreSQL (Sales), and MongoDB (Customers)."
                    "\n\n⚡ OPTIMIZED QUERY MODE:"
                    "\n→ Queries are auto-generated from schema metadata (NOT via LLM)."
                    "\n→ This saves tokens and ensures deterministic, fast queries."
                    "\n→ Your role: understand user intent, execute pre-generated queries, explain results."
                    "\n\nFLOW:"
                    "\n1. User asks a question (e.g., 'Get orders for customer Smith')"
                    "\n2. System parses intent and generates SQL automatically"
                    "\n3. SQL executes against databases"
                    "\n4. You explain/format the results in Markdown"
                    "\n\nCROSS-DATABASE CAPABILITIES:"
                    "\n- Join Sales (Postgres) to Customers (Mongo) using CustomerId"
                    "\n- Join Inventory (SQL) to Sales (Postgres) using ProductId"
                    "\n\nRULES:"
                    "\n1. If query generation fails, explain what additional info is needed."
                    "\n2. For ambiguous queries (lastname='Smith' matches 10+ customers), ask for clarification."
                    "\n3. Present results in clean Markdown TABLES."
                    "\n4. For large result sets (>500 rows), suggest refining the filter."
                    "\n5. Always mention how many rows/records were returned."
                )
                inputs = {
                    "messages": [
                        SystemMessage(content=system_prompt),
                        HumanMessage(content=user_input)
                    ]
                }
                
                async for event in app.astream_events(inputs, version="v2"):
                    kind = event["event"]
                    
                    if kind == "on_chat_model_stream":
                        content = event["data"]["chunk"].content
                        if content:
                            yield {"type": "token", "content": content}
                            
                    elif kind == "on_tool_start":
                        tool_name = event["name"]
                        tool_input = event["data"].get("input")
                        logger.info("\n" + "="*80)
                        logger.info(f"FETCHING DATA FROM DATABASE: {tool_name}")
                        logger.info("="*80)
                        logger.info(f"Input/Query: {json.dumps(tool_input, indent=2, default=str)}")
                        logger.info("="*80 + "\n")
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
                            # We intentionally exclude "output" here to prevent 
                            # payload size crashes during large data lookups.
                        }


    except Exception as e:
        import traceback
        traceback.print_exc()
        yield {"type": "error", "content": str(e)}

