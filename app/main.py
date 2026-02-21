from fastapi import FastAPI
from app.routers import grades, trajectory, reports, conversion, audit

app = FastAPI(title="EduGrade Global API")

app.include_router(grades.router)
app.include_router(trajectory.router)
app.include_router(reports.router)
app.include_router(conversion.router)
app.include_router(audit.router)
