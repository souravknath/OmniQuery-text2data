import traceback, os
from dotenv import load_dotenv
load_dotenv(override=True)
from langchain_openai import ChatOpenAI

try:
    llm = ChatOpenAI(
        api_key=os.getenv('OPENROUTER_API_KEY'), 
        base_url='https://openrouter.ai/api/v1', 
        model='openai/gpt-4o-mini',
        default_headers={
            "HTTP-Referer": "https://localhost:3000",
            "X-Title": "Multi-DB AI Chat (LangGraph)"
        }
    )
    print(llm.invoke('hi'))
except Exception as e:
    traceback.print_exc()
