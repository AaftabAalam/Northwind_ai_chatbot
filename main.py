from fastapi import FastAPI
from connect_northwind_db import chat_with_northwind

app = FastAPI()

@app.post("/ask")
async def ask_northwind(question: str):
    try:
        response = chat_with_northwind(question)
        return {"response": response}
    except Exception as e:
        return {"error": str(e)}