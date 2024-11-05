# main.py
from fastapi import FastAPI
from database import engine
from tasks import router as tasks_router

app = FastAPI()
app.include_router(tasks_router)

@app.get("/")
async def root():
    return {"message": "Data Ingestion and Processing API"}
