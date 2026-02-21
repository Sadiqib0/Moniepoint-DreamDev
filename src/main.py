import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from src.database import init_db, is_data_loaded
from src.ingest import load_all_csvs
from src.analytics import (
    get_top_merchant,
    get_monthly_active_merchants,
    get_product_adoption,
    get_kyc_funnel,
    get_failure_rates
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=== Moniepoint Analytics API — Starting up ===")
    try:
        logger.info("Initialising database schema …")
        init_db()

        if is_data_loaded():
            logger.info("✅ Data already loaded — skipping ingestion. Server ready!")
        else:
            logger.info("Loading CSV data into database …")
            processed, inserted = load_all_csvs()
            logger.info("Startup complete. %d processed / %d inserted.", processed, inserted)

    except Exception as exc:
        logger.error("Startup failed: %s", exc, exc_info=True)
        raise
    yield
    logger.info("=== Moniepoint Analytics API — Shutting down ===")


app = FastAPI(
    title="Moniepoint Analytics API",
    version="1.0.0",
    description="Analytics API for Moniepoint merchant activity data — DreamDev 2026",
    lifespan=lifespan,
)


@app.get("/", tags=["Health"])
def root():
    return {"status": "ok", "service": "Moniepoint Analytics API"}


@app.get("/health", tags=["Health"])
def health():
    return {"status": "healthy"}


@app.get("/analytics/top-merchant", tags=["Analytics"])
def top_merchant():
    try:
        return JSONResponse(content=get_top_merchant())
    except Exception as exc:
        logger.error("/analytics/top-merchant error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/analytics/monthly-active-merchants", tags=["Analytics"])
def monthly_active_merchants():
    try:
        return JSONResponse(content=get_monthly_active_merchants())
    except Exception as exc:
        logger.error("/analytics/monthly-active-merchants error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/analytics/product-adoption", tags=["Analytics"])
def product_adoption():
    try:
        return JSONResponse(content=get_product_adoption())
    except Exception as exc:
        logger.error("/analytics/product-adoption error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/analytics/kyc-funnel", tags=["Analytics"])
def kyc_funnel():
    try:
        return JSONResponse(content=get_kyc_funnel())
    except Exception as exc:
        logger.error("/analytics/kyc-funnel error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/analytics/failure-rates", tags=["Analytics"])
def failure_rates():
    try:
        return JSONResponse(content=get_failure_rates())
    except Exception as exc:
        logger.error("/analytics/failure-rates error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")