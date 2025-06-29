from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from typing_extensions import TypedDict, Annotated
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.tools.sql_database.tool import QuerySQLDatabaseTool

from langchain_core.tools import tool
import sqlite3
import re
import os

try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

try:
    import mysql.connector
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False

class State(TypedDict):
    question: str
    reasoning: str
    query: str
    validation_result: str
    result: str
    answer: str
    iteration: int

class QueryOutput(TypedDict):
    query: Annotated[str, ..., "Syntactically valid SQL query."]

class ReasoningOutput(TypedDict):
    reasoning: Annotated[str, ..., "Step-by-step reasoning about the question and how to approach it."]
    query: Annotated[str, ..., "Syntactically valid SQL query based on the reasoning."]

db_uri = os.getenv('DATABASE_URI', 'sqlite:///league_players.db')
db = SQLDatabase.from_uri(db_uri)

llm = ChatOpenAI(
    base_url="http://localhost:11434/v1",
    api_key="ollama",
    model="qwen3",
)

@tool(description="Get the complete database schema including all tables and their columns.")
def get_database_schema() -> str:
    return db.get_table_info()

def validate_sql_syntax(query: str, dialect: str) -> str:
    if not query.strip():
        return "Query is empty."
    
    try:
        query_upper = query.upper()
        
        if not any(keyword in query_upper for keyword in ['SELECT', 'FROM']):
            return "Query must contain SELECT and FROM clauses."
        
        dangerous_patterns = [
            r'DROP\s+TABLE',
            r'DELETE\s+FROM',
            r'UPDATE\s+.*\s+SET',
            r'INSERT\s+INTO',
            r'CREATE\s+TABLE',
            r'ALTER\s+TABLE'
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, query_upper):
                return f"Query contains potentially dangerous operation: {pattern}"
        
        if dialect == 'sqlite':
            conn = sqlite3.connect('league_players.db')
            cursor = conn.cursor()
            try:
                clean_query = query.strip()
                if clean_query.endswith(';'):
                    clean_query = clean_query[:-1]
                cursor.execute(f"EXPLAIN {clean_query}")
                cursor.fetchall()
                conn.close()
                return "Query syntax is valid."
            except sqlite3.Error as e:
                conn.close()
                return f"SQL syntax error: {str(e)}"
        
        elif dialect == 'postgresql':
            if not PSYCOPG2_AVAILABLE:
                return "PostgreSQL driver (psycopg2) not available. Install with: pip install psycopg2-binary"
            conn_str = os.getenv('POSTGRES_CONNECTION_STRING')
            if not conn_str:
                return "PostgreSQL connection string not configured. Set POSTGRES_CONNECTION_STRING environment variable."
            conn = psycopg2.connect(conn_str)
            cursor = conn.cursor()
            try:
                cursor.execute(f"EXPLAIN {query}")
                cursor.fetchall()
                conn.close()
                return "Query syntax is valid."
            except psycopg2.Error as e:
                conn.close()
                return f"SQL syntax error: {str(e)}"
        
        elif dialect == 'mysql':
            if not MYSQL_AVAILABLE:
                return "MySQL driver (mysql-connector-python) not available. Install with: pip install mysql-connector-python"
            host = os.getenv('MYSQL_HOST', 'localhost')
            user = os.getenv('MYSQL_USER', 'root')
            password = os.getenv('MYSQL_PASSWORD', '')
            database = os.getenv('MYSQL_DATABASE', 'test')
            
            conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            cursor = conn.cursor()
            try:
                cursor.execute(f"EXPLAIN {query}")
                cursor.fetchall()
                conn.close()
                return "Query syntax is valid."
            except mysql.connector.Error as e:
                conn.close()
                return f"SQL syntax error: {str(e)}"
        
        elif dialect == 'mssql':
            if not PYODBC_AVAILABLE:
                return "SQL Server driver (pyodbc) not available. Install with: pip install pyodbc and install ODBC drivers"
            conn_str = os.getenv('MSSQL_CONNECTION_STRING')
            if not conn_str:
                return "SQL Server connection string not configured. Set MSSQL_CONNECTION_STRING environment variable."
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            try:
                cursor.execute(f"SET SHOWPLAN_TEXT ON; {query}")
                cursor.fetchall()
                conn.close()
                return "Query syntax is valid."
            except pyodbc.Error as e:
                conn.close()
                return f"SQL syntax error: {str(e)}"
        
        else:
            return f"Database dialect '{dialect}' not supported for syntax validation."
            
    except Exception as e:
        return f"Validation error: {str(e)}"

@tool(description="Validate if a SQL query is syntactically correct and uses valid tables/columns.")
def validate_sql_query(query: str) -> str:
    return validate_sql_syntax(query, db.dialect)

@tool(description="Test execute a query with LIMIT 1 to check if it works without returning too much data.")
def test_query_execution(query: str) -> str:
    if not query.strip():
        return "Query is empty."
    
    try:
        test_query = query.strip()
        
        if test_query.endswith(';'):
            test_query = test_query[:-1]
        
        if db.dialect == 'sqlite':
            if 'LIMIT' not in test_query.upper():
                test_query += " LIMIT 1"
        elif db.dialect == 'postgresql':
            if 'LIMIT' not in test_query.upper():
                test_query += " LIMIT 1"
        elif db.dialect == 'mysql':
            if 'LIMIT' not in test_query.upper():
                test_query += " LIMIT 1"
        elif db.dialect == 'mssql':
            if 'TOP' not in test_query.upper():
                test_query = test_query.replace('SELECT', 'SELECT TOP 1', 1)
        
        execute_query_tool = QuerySQLDatabaseTool(db=db)
        result = execute_query_tool.invoke(test_query)
        return f"Query test successful. Sample result: {result}"
    except Exception as e:
        return f"Query execution failed: {str(e)}"

react_system_message = """
You are a ReAct (Reasoning + Acting) agent that helps users query a database. You follow this process:

1. REASON: Think step-by-step about the user's question and how to approach it
2. ACT: Generate a SQL query based on your reasoning
3. OBSERVE: The system will validate and execute your query

Available tools:
- get_database_schema: Get the complete database schema
- validate_sql_query: Check if a SQL query is syntactically correct
- test_query_execution: Test execute a query with LIMIT 1

Database dialect: {dialect}
Table information: {table_info}

CRITICAL SQL GUIDELINES:
- Use standard SQL syntax for {dialect}
- For winrate calculations: use (wins * 100.0 / (wins + losses)) or (wins * 100.0 / total_matches)
- DO NOT use non-existent functions like WIN_PERCENTAGE, WIN_PERC, etc.
- For complex questions with multiple parts, generate multiple SQL queries separated by semicolons
- Keep queries simple and efficient
- Use proper decimal notation (0.52 for 52%, not 52.0)
- Always use valid table and column names from the schema

IMPORTANT: When providing reasoning, use exactly ONE set of think tags like this:
<think>Your reasoning here</think>

Do NOT use multiple closing tags or nested think tags. Use only one opening and one closing tag.
"""

def reason_and_plan(state: State):
    prompt = ChatPromptTemplate.from_messages([
        ("system", react_system_message),
        ("user", "Question: {question}\n\nFirst, reason about this question step-by-step and plan how to approach it. If the question has multiple parts, break it down and plan separate queries for each part. For multi-part questions, you MUST generate multiple SQL queries separated by semicolons. Use <think> tags for your internal reasoning.")
    ])
    
    reasoning_prompt = prompt.invoke({
        "dialect": db.dialect,
        "table_info": db.get_table_info(),
        "question": state["question"]
    })
    
    try:
        structured_llm = llm.with_structured_output(ReasoningOutput)
        result = structured_llm.invoke(reasoning_prompt)
        
        return {
            "reasoning": result.get("reasoning", "No reasoning provided"),
            "query": result.get("query", "")
        }
    except Exception as e:
        fallback_response = llm.invoke(reasoning_prompt)
        return {
            "reasoning": f"Structured output failed: {str(e)}. Fallback response: {fallback_response.content}",
            "query": ""
        }

def validate_and_refine(state: State):
    validation_result = validate_sql_query.invoke(state["query"])
    
    if "valid" in validation_result.lower() or "successful" in validation_result.lower():
        test_result = test_query_execution.invoke(state["query"])
        if "failed" in test_result.lower():
            return {
                "query": state["query"],
                "validation_result": f"Validation: {validation_result}\nTest: {test_result}\nNote: Query may need manual refinement."
            }
        else:
            return {"validation_result": f"Validation: {validation_result}\nTest: {test_result}"}
    else:
        return {
            "query": state["query"],
            "validation_result": f"Validation failed: {validation_result}\nUsing original query."
        }

def execute_final_query(state: State):
    if not state["query"].strip():
        return {"result": "No query to execute - question cannot be answered with available schema."}
    
    try:
        execute_query_tool = QuerySQLDatabaseTool(db=db)
        
        queries = [q.strip() for q in state["query"].split(';') if q.strip()]
        
        if len(queries) == 1:
            result = execute_query_tool.invoke(queries[0])
            return {"result": result}
        else:
            results = []
            for i, query in enumerate(queries, 1):
                try:
                    result = execute_query_tool.invoke(query)
                    results.append(f"Query {i}: {result}")
                except Exception as e:
                    results.append(f"Query {i} failed: {str(e)}")
            
            return {"result": "\n".join(results)}
            
    except Exception as e:
        return {"result": f"Execution failed: {str(e)}"}

def generate_final_answer(state: State):
    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a helpful assistant that explains database query results in a clear, user-friendly way. Provide direct answers to the user's questions without showing internal reasoning or technical details in the final answer."),
        ("user", """
Given the following information, provide a clear and direct answer to the user's question:

Question: {question}
SQL Query: {query}
Query Result: {result}

Provide a simple, direct answer that:
- Answers the user's question clearly
- Uses the actual query results
- Is written in plain English
- Does NOT show SQL queries or technical details
- Does NOT show internal reasoning or think tags
- Focuses on what the user asked for

If the query failed, simply explain why the question cannot be answered.
""")
    ])
    
    answer_prompt = prompt.invoke({
        "question": state["question"],
        "query": state["query"],
        "result": state["result"]
    })
    
    response = llm.invoke(answer_prompt)
    return {"answer": response.content}

from langgraph.graph import START, StateGraph

workflow = StateGraph(State)

workflow.add_node("reason_and_plan", reason_and_plan)
workflow.add_node("validate_and_refine", validate_and_refine)
workflow.add_node("execute_final_query", execute_final_query)
workflow.add_node("generate_final_answer", generate_final_answer)

workflow.add_edge(START, "reason_and_plan")
workflow.add_edge("reason_and_plan", "validate_and_refine")
workflow.add_edge("validate_and_refine", "execute_final_query")
workflow.add_edge("execute_final_query", "generate_final_answer")

graph = workflow.compile()

def run_react_agent(question: str):
    print("=" * 50)
    
    for step in graph.stream(
        {"question": question, "iteration": 0},
        stream_mode="updates"
    ):
        node_name = list(step.keys())[0]
        node_output = step[node_name]
        
        if node_name == "reason_and_plan":
            print("REASONING STEP:")
            print(f"Reasoning: {node_output.get('reasoning', 'N/A')}")
            print(f"Initial Query: {node_output.get('query', 'N/A')}")
            print("-" * 30)
            
        elif node_name == "validate_and_refine":
            print("VALIDATION STEP:")
            print(f"Validation Result: {node_output.get('validation_result', 'N/A')}")
            if 'query' in node_output:
                print(f"Refined Query: {node_output['query']}")
            print("-" * 30)
            
        elif node_name == "execute_final_query":
            print("EXECUTION STEP:")
            print(f"Result: {node_output.get('result', 'N/A')}")
            print("-" * 30)
            
        elif node_name == "generate_final_answer":
            print("FINAL ANSWER:")
            print(f"Answer: {node_output.get('answer', 'N/A')}")
            print("=" * 50)

def extract_think_tags(text):
    think_pattern = r'<think>(.*?)</think>'
    thoughts = re.findall(think_pattern, text, re.DOTALL)
    return thoughts

def remove_think_tags(text):
    if not text:
        return ""
    cleaned = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL).strip()
    cleaned = re.sub(r'</think>+', '', cleaned)
    cleaned = re.sub(r'<think>+', '', cleaned)
    return cleaned.strip()

if __name__ == "__main__":
    print("Interactive Mode - Ask your questions!")
    print("Type 'quit' to exit")
    
    while True:
        try:
            user_question = input("\nEnter your question: ").strip()
            if user_question.lower() in ['quit', 'exit', 'q']:
                break
            if user_question:
                run_react_agent(user_question)
        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"Error: {e}")