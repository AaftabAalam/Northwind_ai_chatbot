from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from groq import Groq
import os
import re
import io
import csv

load_dotenv()
groq_key = os.getenv("GROQ_KEY")
client  = Groq(api_key=groq_key)

def clean_sql(sql: str) -> str:
    sql = sql.strip()
    sql = re.sub(r"^```(?:sql)?\s*", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\s*```$", "", sql)
    return sql.strip()

engine = create_engine(
    "sqlite:////Users/apple/ml-setup/blue_yonder/"
    "northwind-SQLite3/dist/northwind.db"
)

def get_table_columns(engine):
    with engine.connect() as conn:
        tables = [
            row[0]
            for row in conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table';"
            ))
        ]
        schema = {}
        for table in tables:
            cols = conn.execute(text(f'PRAGMA table_info("{table}");'))
            schema[table] = [col[1] for col in cols]
        return schema

schema = get_table_columns(engine)

def schema_to_string(schema: dict) -> str:
    lines = []
    for table, cols in schema.items():
        lines.append(f"{table}({', '.join(cols)})")
    return "\n".join(lines)

schema_description = schema_to_string(schema)


PROMPT_TEMPLATE = """
You are an AI assistant that writes valid SQLite SQL queries given:
Database schema:
{schema}
User question:
{question}
Output only the SQL, without explanation.
""".strip()


def generate_sql_with_groq(question: str, schema_description: str) -> str:
    prompt = PROMPT_TEMPLATE.format(
        schema=schema_description,
        question=question
    )
    completion = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0,
        max_completion_tokens=512,
        top_p=1.0,
        stream=False
    )
    return completion.choices[0].message.content.strip()


def execute_sql(engine, sql: str):
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        return [dict(row._mapping) for row in result]

EXPLANATION_PROMPT = """
You are an assistant helping users understand database query results.
Below is the original user question:
{question}
Here is the SQL query that was executed:
{sql}
And here is the result of the query (tabular output):
{result}
Explain the output in clear, concise, human-readable language without adding your comments and any additional explanation, just explain the incoming tabular output.
""".strip()


def explain_result_with_groq(question: str, sql: str, result: str) -> str:
    prompt = EXPLANATION_PROMPT.format(
        question=question,
        sql=sql,
        result=result
    )
    completion = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7,
        max_completion_tokens=2048,
        top_p=1.0,
        stream=False
    )
    return completion.choices[0].message.content.strip()


def chat_with_northwind(question: str):
    sql = generate_sql_with_groq(question, schema_description)
    sql = clean_sql(sql)
    try:
        rows = execute_sql(engine, sql)
        if not rows:
            return "No results found."

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        
        headers = list(rows[0].keys())
        writer.writerow(headers)
        for row in rows:
            writer.writerow([row[h] for h in headers])
        
        tabular_output = buffer.getvalue().strip()
        buffer.close()

        explanation = explain_result_with_groq(question, sql, tabular_output)
        return f"Tabular output: \n{tabular_output}\n\n Explanation:\n{explanation}"
    except Exception as e:
        return f"Error executing SQL: {e}"