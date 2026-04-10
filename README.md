# OmniQuery-OmniQuery

OmniQuery-OmniQuery is a sophisticated multi-database AI chat application that enables users to query diverse data sources (HR, Sales, Marketing, Inventory) using natural language. It leverages a modern stack combining FastAPI, Angular, LangGraph, and the Model Context Protocol (MCP) to provide a seamless, real-time data exploration experience.

---

## 🏗️ Architecture

The application follows a modular architecture designed for scalability and security:

- **Frontend**: A modern Angular (v19) application providing a rich, responsive chat interface with real-time streaming capabilities.
- **Backend API**: A FastAPI server that handles chat requests and orchestrates the AI agent logic.
- **AI Agent (LangGraph)**: A stateful AI agent built with LangGraph that uses reasoning to decide which tools to call and how to process data.
- **MCP Server**: Implements the Model Context Protocol (MCP) to provide a secure and standardized interface for database interactions.
- **Data Layers**:
    - **NoSQL (MongoDB)**: Stores customer profiles, activities, and support tickets.
    - **SQL (Planned/Mocked)**: For Sales, Marketing, HR, and Inventory datasets (configured via environment connections).

---

## ✨ Core Features

- **Natural Language Data Querying**: Query complex datasets without knowing SQL or NoSQL syntax.
- **Multi-Database Support**: Securely interact with multiple databases through a single interface.
- **Real-Time Streaming**: Tokens and tool activity events are streamed to the UI using NDJSON for a "live" feel.
- **MPC Integrated Tools**: Dynamically loaded tools from the MCP server ensure a clean separation between the LLM and the data layer.
- **Dynamic Reasoning**: Uses state-of-the-art LLMs (via Groq or OpenAI/OpenRouter) to handle multi-step data tasks.

---

## 🛠️ Setup & Installation

### Prerequisites

- **Python 3.10+**
- **Node.js (LTS)** & **npm**
- **MongoDB** (Running locally or accessible via URI)
- **SQL Server** (Running locally or accessible via URI)

### Backend Setup

1. **Navigate to backend directory**:
   ```bash
   cd backend
   ```

2. **Create and activate a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**:
   Create a `.env` file based on `.env.example`:
   ```bash
   cp .env.example .env
   ```
   *Edit `.env` to include your API keys and database connection strings.*

### Database Initialization

1. **Seed the MongoDB database**:
   ```bash
   python seed_customer_db.py
   ```
   *This will populate your local MongoDB with sample customer, activity, and ticket data.*

### Frontend Setup

1. **Navigate to frontend directory**:
   ```bash
   cd frontend
   ```

2. **Install dependencies**:
   ```bash
   npm install
   ```

3. **Start the development server**:
   ```bash
   npm start
   ```
   *The app will be available at `http://localhost:4200`.*

---

## 🚀 Running the Application

1. **Start the FastAPI Backend**:
   From the `backend` directory:
   ```bash
   python main.py
   ```
   *The API will run on `http://localhost:8000`.*

2. **Interact with the AI**:
   Open your browser to the frontend URL and start asking questions about your data!

---

## ⚙️ Environment Variables (Backend)

| Variable | Description | Default |
|----------|-------------|---------|
| `OPENROUTER_API_KEY` | API Key for OpenRouter (if using) | - |
| `GROQ_API_KEY` | API Key for Groq (if using) | - |
| `MODEL_PROVIDER` | `groq` or `openrouter` | `openrouter` |
| `MODEL_NAME` | The model identifier (e.g., `llama3-70b-8192` or `openai/gpt-4o-mini`) | `openai/gpt-4o-mini` |
| `MONGO_URI` | MongoDB connection string | `mongodb://localhost:27017/` |
| `SALES_DB_CONN` | SQL connection string for Sales DB | - |
| `HR_DB_CONN` | SQL connection string for HR DB | - |

---

## 📄 License

This project is licensed under the MIT License.
