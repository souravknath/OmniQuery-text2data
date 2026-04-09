import warnings
warnings.filterwarnings("ignore")

import os
import sys
from typing import TypedDict, Annotated, List
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.messages import BaseMessage, HumanMessage
from langgraph.graph import StateGraph, END, START
from langgraph.prebuilt import ToolNode
from langgraph.graph.message import add_messages
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from langchain_mcp_adapters.tools import load_mcp_tools
from langchain_groq import ChatGroq

load_dotenv()

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
        # Setup MCP Client
        server_params = StdioServerParameters(
            command=sys.executable,
            args=["mcp_server.py"],
        )
        
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                
                # Fetch tools dynamically
                tools = await load_mcp_tools(session)
                tool_node = ToolNode(tools)
                
                # Configure LLM based on provider
                provider = os.getenv("MODEL_PROVIDER", "openrouter").lower()
                model_name = os.getenv("MODEL_NAME", "openai/gpt-4o-mini")
                
                if provider == "groq":
                    llm = ChatGroq(
                        api_key=os.getenv("GROQ_API_KEY"),
                        model=model_name,
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
                    response = await llm.ainvoke(messages)
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
                inputs = {"messages": [HumanMessage(content=user_input)]}
                
                async for event in app.astream_events(inputs, version="v2"):
                    kind = event["event"]
                    
                    if kind == "on_chat_model_stream":
                        content = event["data"]["chunk"].content
                        if content:
                            yield {"type": "token", "content": content}
                            
                    elif kind == "on_tool_start":
                        yield {
                            "type": "tool_start", 
                            "tool": event["name"], 
                            "input": event["data"].get("input")
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

