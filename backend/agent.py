import warnings
warnings.filterwarnings("ignore")

import os
import sys
import json
import logging
import tiktoken
from typing import TypedDict, Annotated, List
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
file_handler = logging.FileHandler(log_file, encoding='utf-8')
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
                
                async def call_model(state: AgentState):
                    """Main reasoning node."""
                    messages = state['messages']
                    
                    # Estimate token count and truncate if needed
                    total_tokens = 0
                    for msg in messages:
                        content = msg.content if isinstance(msg.content, str) else str(msg.content)
                        total_tokens += estimate_tokens(content)
                    
                    # Load token limit from env or default to a safe 128k context window
                    max_tpm = int(os.getenv("MAX_TOKEN_LIMIT", "128000")) 
                    safe_threshold = int(max_tpm * 0.95)  # Use 95% to be safe
                    
                    logger.info("\n" + "="*80)
                    logger.info(f"TOKEN COUNT PRE-FLIGHT CHECK")
                    logger.info("="*80)
                    logger.info(f"Estimated total tokens: {total_tokens}")
                    logger.info(f"Rate limit (TPM): {max_tpm}")
                    logger.info(f"Safe threshold (95%): {safe_threshold}")
                    
                    # If exceeding limit, keep only system prompt + last 2 messages
                    if total_tokens > safe_threshold:
                        logger.warning(f"\nWARNING: TOKEN COUNT EXCEEDS SAFE THRESHOLD!")
                        logger.warning(f"Reducing message history to prevent rate limit...\n")
                        
                        # Find system prompt (usually first)
                        system_msgs = [m for m in messages if isinstance(m, SystemMessage)]
                        other_msgs = [m for m in messages if not isinstance(m, SystemMessage)]
                        
                        reduced_messages = system_msgs.copy()
                        
                        # Always keep the first human message (the initial objective)
                        if other_msgs and isinstance(other_msgs[0], HumanMessage):
                            reduced_messages.append(other_msgs[0])
                            other_msgs = other_msgs[1:]
                            
                        # And keep the last two messages (typically tool thought + tool output)
                        if len(other_msgs) > 2:
                            reduced_messages.extend(other_msgs[-2:])
                        else:
                            reduced_messages.extend(other_msgs)
                            
                        logger.info(f"Original messages: {len(messages)}, Reduced to: {len(reduced_messages)}")
                        messages = reduced_messages
                        
                        # Recalculate
                        total_tokens = 0
                        for msg in messages:
                            content = msg.content if isinstance(msg.content, str) else str(msg.content)
                            total_tokens += estimate_tokens(content)
                            
                        # If still over limit due to massive tool output, aggressively truncate the last message
                        if total_tokens > safe_threshold and messages:
                            last_msg = messages[-1]
                            logger.warning(f"Still over threshold ({total_tokens} > {safe_threshold}). Truncating last message...")
                            
                            # Calculate roughly how many characters we need to chop
                            # We'll just take a 4x multiplier of the safe threshold remaining
                            other_tokens = sum(estimate_tokens(str(m.content)) for m in messages[:-1])
                            remaining_tokens = max(1000, safe_threshold - other_tokens)
                            allowed_chars = int(remaining_tokens * 3.5)  # conservative chars per token
                            
                            content_str = str(last_msg.content)
                            if len(content_str) > allowed_chars:
                                if hasattr(last_msg, 'content'):
                                    last_msg.content = content_str[:allowed_chars] + "\n\n...[TRUNCATED DUE TO TOKEN LIMIT]..."
                                elif isinstance(last_msg, dict) and 'content' in last_msg:
                                    last_msg['content'] = content_str[:allowed_chars] + "\n\n...[TRUNCATED DUE TO TOKEN LIMIT]..."
                                    
                            total_tokens = sum(estimate_tokens(str(m.content)) for m in messages)
                            
                        logger.info(f"Adjusted token count: {total_tokens}\n")
                    
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
                    
                    # Log tool calls explicitly if they exist
                    if hasattr(response, 'tool_calls') and response.tool_calls:
                        logger.info("\n" + "!"*80)
                        logger.info("LLM DECIDED TO USE TOOLS:")
                        for tc in response.tool_calls:
                            logger.info(f" - {tc['name']}({tc['args']})")
                        logger.info("!"*80 + "\n")
                    
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
                    "\n\nCRITICAL: You must call 'get_database_info' FIRST to see the table structures and relationships."
                    "\n\nCROSS-DATABASE CAPABILITIES:"
                    "\n- Join Sales (Postgres) to Customers (Mongo) using CustomerId mappings."
                    "\n- Join Inventory (SQL) to Sales (Postgres) using ProductId mappings."
                    "\n\nGUIDELINES:"
                    "\n1. ALWAYS inspect the schema via 'get_database_info' before writing any queries."
                    "\n2. Present final merged results in a clean Markdown TABLE."
                    "\n3. Handle reserved keywords by quoting: [SQL Server] or \"PostgreSQL\"."
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
                        
                        # Prominently print the actual LLM generated query
                        actual_query = None
                        if isinstance(tool_input, dict):
                            actual_query = tool_input.get("sql_query") or tool_input.get("query_payload")
                        
                        if actual_query:
                            header = f"ACTUAL LLM GENERATED QUERY ({tool_name})"
                            print(f"\n\033[96m{'='*80}\n{header}\n{'-'*80}\n{actual_query}\n{'='*80}\033[0m\n")
                            logger.info(f"\n{header}:\n{actual_query}\n")

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

