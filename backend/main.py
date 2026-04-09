from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from agent import run_agent
import uvicorn
import json

app = FastAPI(title="Multi-DB AI Chat API")

# Setup CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ChatRequest(BaseModel):
    message: str

@app.post("/chat")
async def chat(req: ChatRequest):
    """
    Streaming endpoint for the chat interface.
    Yields JSON chunks for tokens and tool activities.
    """
    if not req.message:
        raise HTTPException(status_code=400, detail="Message cannot be empty")
        
    async def event_generator():
        try:
            async for chunk in run_agent(req.message):
                # Yield as JSON string with a newline for easy parsing
                yield json.dumps(chunk) + "\n"
        except Exception as e:
            print(f"Streaming error: {e}")
            yield json.dumps({"type": "error", "content": "A streaming error occurred."}) + "\n"

    return StreamingResponse(event_generator(), media_type="application/x-ndjson")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

