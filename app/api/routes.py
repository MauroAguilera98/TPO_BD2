from fastapi import APIRouter
from app.db.mongo import db

router = APIRouter()

@router.get("/mongo-test")
async def mongo_test():
    await db.test.insert_one({"status": "ok"})
    return {"mongo": "connected"}
