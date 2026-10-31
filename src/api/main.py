from fastapi import Depends, FastAPI

from src.api.dependencies import get_api_key
from src.api.v1 import payments
from src.logging_config import setup_logging

setup_logging()

app = FastAPI(
    title="Payment Processing Service",
)


@app.get("/health", tags=["Health"])
async def health_check():
    return {"status": "ok"}


app.include_router(
    payments.router, prefix="/api/v1", dependencies=[Depends(get_api_key)]
)
