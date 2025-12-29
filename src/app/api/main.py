from fastapi import FastAPI
from .routes import router

app = FastAPI(title="KOSPI S-RIM (Quarterly Snapshot)")

app.include_router(router)

@app.get("/health")
def health():
    return {"ok": True}
