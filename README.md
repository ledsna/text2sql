# ReAct SQL Agent

A clean, efficient Text-to-SQL agent that converts natural language questions into database queries using the ReAct (Reasoning + Acting) pattern.

## Features

- **Natural Language to SQL**: Ask questions in plain English, get database answers
- **ReAct Pattern**: Transparent 4-step reasoning process
- **Clean Web Interface**: Modern Streamlit UI with expandable reasoning details
- **Conversation History**: Persistent chat interface
- **Automatic Ollama Integration**: Seamless local LLM setup with qwen3
- **Multi-Database Support**: SQLite, PostgreSQL, MySQL, SQL Server

## Quick Start

### Prerequisites
- Python 3.12+
- [Ollama](https://ollama.ai/) installed locally
- [uv](https://github.com/astral-sh/uv) package manager

### Installation

1. **Clone and setup**:
```bash
git clone <repository-url>
cd llmp
uv sync
```

2. **Start the app**:
```bash
uv run streamlit run main.py
```

3. **Open your browser** to http://localhost:8501

The app will automatically start Ollama with the qwen3 model if it's not already running.

## How It Works

### ReAct Pattern
The agent follows a 4-step reasoning process:

1. **REASONING STEP**: Analyzes your question and plans the SQL approach
2. **VALIDATION STEP**: Validates SQL syntax and security
3. **EXECUTION STEP**: Safely executes the query against the database
4. **FINAL ANSWER**: Provides a clean, user-friendly answer

### Example
**Question**: "How many players have a winrate above 52%?"

**Process**:
- Agent reasons about winrate calculations
- Generates SQL: `SELECT COUNT(*) FROM league_players WHERE (wins * 100.0 / (wins + losses)) > 52`
- Validates and executes the query
- Returns: "There are 6,923 players with a winrate above 52%."

## Database

The default database is `league_players.db` (SQLite) containing League of Legends player statistics. You can configure other databases using environment variables:

```bash
# SQLite (default)
DATABASE_URI=sqlite:///league_players.db

# PostgreSQL
DATABASE_URI=postgresql://user:pass@localhost/dbname

# MySQL
DATABASE_URI=mysql://user:pass@localhost/dbname

# SQL Server
DATABASE_URI=mssql://user:pass@localhost/dbname
```

## Architecture

### Core Components
- **LangGraph Workflow**: Orchestrates the ReAct process
- **Structured Output**: Type-safe LLM responses using TypedDict
- **SQL Validation**: Multi-layer security and syntax validation
- **Streamlit UI**: Clean interface with expandable reasoning details

### Security Features
- **SQL Injection Prevention**: Regex-based dangerous operation detection
- **Syntax Validation**: Database-specific validation using EXPLAIN
- **Safe Execution**: Test execution with LIMIT clauses
- **Input Sanitization**: Comprehensive query validation pipeline

## Usage

### Web Interface (Recommended)
```bash
uv run streamlit run main.py
```

### Command Line
```bash
uv run python chat.py
```

## Technical Details

### State Management
```python
class State(TypedDict):
    question: str
    reasoning: str
    query: str
    validation_result: str
    result: str
    answer: str
    iteration: int
```

### LangGraph Workflow
```python
workflow = StateGraph(State)
workflow.add_node("reason_and_plan", reason_and_plan)
workflow.add_node("validate_and_refine", validate_and_refine)
workflow.add_node("execute_final_query", execute_final_query)
workflow.add_node("generate_final_answer", generate_final_answer)
```

### SQL Validation Pipeline
1. **Basic Syntax Check**: Validates SELECT/FROM clauses
2. **Security Scan**: Detects dangerous operations (DROP, DELETE, etc.)
3. **Database Validation**: Uses EXPLAIN for syntax verification
4. **Safe Test Execution**: Runs with LIMIT 1 for safety

## Dependencies

Key dependencies managed by `pyproject.toml`:
- `langchain-openai`: LLM integration
- `langchain-community`: Database tools
- `langgraph`: Workflow orchestration
- `streamlit`: Web interface
- `sqlite3`: Default database (included with Python)

Optional database drivers:
- `psycopg2-binary`: PostgreSQL support
- `mysql-connector-python`: MySQL support
- `pyodbc`: SQL Server support

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with `uv run streamlit run main.py`
5. Submit a pull request

## License

MIT License - see LICENSE file for details.