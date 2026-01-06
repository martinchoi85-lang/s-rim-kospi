from fastapi import FastAPI
from .routes import router
from app.api.routes_srim import router as srim_router  # SW 관점: SRIM 라우터 등록

app = FastAPI(title="KOSPI S-RIM (Quarterly Snapshot)")

app.include_router(router)
app.include_router(srim_router)  # SW 관점: 엔드포인트 활성화

@app.get("/health")
def health():
    return {"ok": True}
