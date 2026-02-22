from fastapi import FastAPI
from app.routers import grades, trajectory, reports, conversion, audit
from app.db.neo4j import init_neo4j_schema 

app = FastAPI(title="EduGrade Global API")

@app.on_event("startup")
async def startup_event():
    print("🚀 Iniciando sistema EduGrade...")
    # Disparamos la creación de índices en Neo4j automáticamente
    await init_neo4j_schema()

app.include_router(grades.router)
app.include_router(trajectory.router)
app.include_router(reports.router)
app.include_router(conversion.router)
app.include_router(audit.router)
