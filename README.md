# Moniepoint Analytics API — DreamDev 2026

**Candidate:** [Sadiq Ibrahim Umar]  
**Language / Framework:** Python 3.11+ · FastAPI · PostgreSQL  
**Port:** 8080
## Project Structure

```
moniepoint-analytics/
├── src/
│   ├── __init__.py        # Package marker
│   ├── config.py          # Environment variable loading
│   ├── database.py        # DB connection + schema initialisation
│   ├── ingest.py          # CSV → PostgreSQL pipeline
│   ├── analytics.py       # SQL queries for each endpoint
│   ├── main.py            # FastAPI app + route handlers
│   └── seed_sample.py     # (Optional) generate sample CSV for testing
├── data/                  # Place your activities_YYYYMMDD.csv files here
├── .env.example           # Template for environment variables
├── requirements.txt
└── README.md
```

---

## Prerequisites

| Tool | Version |
|------|---------|
| Python | 3.11+ |
| PostgreSQL | 14+ |
| pip | latest |

---

## Setup Instructions

### 1 · Clone the repository

```bash
git clone <your-repo-url>
cd Moniepoint-DreamDev
```

### 2 · Create a Python virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows
```

### 3 · Install dependencies

```bash
pip install -r requirements.txt
```

### 4 · Create the PostgreSQL database

Open `psql` (or any PostgreSQL client) and run:

```sql
CREATE DATABASE moniepoint_analytics;
```

### 5 · Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and fill in your values:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=moniepoint_analytics
DB_USER=postgres
DB_PASSWORD=postgres
DATA_DIR=./data
```

### 6 · Place CSV files

Extract the provided `data.zip` into the `data/` folder so it looks like:

```
data/
├── activities_20240101.csv
├── activities_20240102.csv
└── ...
```

> **No real data yet?** Generated a sample dataset for smoke-testing:
> ```bash
> python -m src.seed_sample
> ```

### 7 · Start the API

```bash
uvicorn src.main:app --host 0.0.0.0 --port 8080
```

On first boot the application will:
1. Create the `merchant_activities` table and indexes (idempotent).
2. Read every `activities_YYYYMMDD.csv` from `DATA_DIR`.
3. Clean / validate each row and bulk-insert into PostgreSQL.
4. Begin serving requests.

Startup time depends on dataset size (typically under 5 minutes for a year of data).

---

## API Reference

Base URL: `http://localhost:8080`

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/analytics/top-merchant` | Merchant with highest successful transaction volume |
| GET | `/analytics/monthly-active-merchants` | Unique active merchants per month |
| GET | `/analytics/product-adoption` | Unique merchant count per product |
| GET | `/analytics/kyc-funnel` | KYC conversion funnel counts |
| GET | `/analytics/failure-rates` | Failure rate per product (%) |
| GET | `/health` | Health check |
| GET | `/docs` | Interactive Swagger UI |

### Example responses

**`GET /analytics/top-merchant`**
```json
{"merchant_id": "MRC-001234", "total_volume": 98765432.10}
```

**`GET /analytics/monthly-active-merchants`**
```json
{"2024-01": 8234, "2024-02": 8456, "2024-12": 9102}
```

**`GET /analytics/product-adoption`**
```json
{"POS": 15234, "AIRTIME": 12456, "BILLS": 10234}
```

**`GET /analytics/kyc-funnel`**
```json
{"documents_submitted": 5432, "verifications_completed": 4521, "tier_upgrades": 3890}
```

**`GET /analytics/failure-rates`**
```json
[{"product": "BILLS", "failure_rate": 5.2}, {"product": "AIRTIME", "failure_rate": 4.1}]
```

---
“Output depends on available CSV months.”
######
**🧪 Testing with curl**

The API can be tested directly from the command line using curl, without any additional tools or authentication.

Ensure the service is running on port 8080

All endpoints use the GET method

No authentication is required

All responses are returned as application/json
--------------
Example Requests
### 🧪 Quick Test Commands

| Feature | Command |
| :--- | :--- |
| **Top Merchant** | `curl http://localhost:8080/analytics/top-merchant` |
| **Active Users** | `curl http://localhost:8080/analytics/monthly-active-merchants` |
| **Adoption** | `curl http://localhost:8080/analytics/product-adoption` |
| **KYC Funnel** | `curl http://localhost:8080/analytics/kyc-funnel` |
| **Failure Rates** | `curl http://localhost:8080/analytics/failure-rates` |

**Response Notes**

-Monetary values are formatted to 2 decimal places

-Percentage values are formatted to 1 decimal place

-PENDING events are excluded where specified

-Output depends on the CSV files available in the data/ directory

Successful responses return HTTP 200 OK
## Assumptions

1. **Monetary precision:** amounts are stored and returned with 2 decimal places; failure rates with 1 decimal place — matching the specification.
2. **Idempotent ingestion:** `ON CONFLICT (event_id) DO NOTHING` prevents duplicate rows if the server restarts. Safe to restart without dropping data.
3. **Malformed rows:** any row with an invalid UUID, unknown product, unknown status, un-parseable timestamp, or empty `merchant_id` is silently skipped and logged.
4. **KYC funnel:** counts distinct `merchant_id` per event_type (not per event) so that a merchant who submitted multiple documents counts once.
5. **PENDING excluded from failure rates:** per specification — only `SUCCESS` and `FAILED` are considered.
6. **Timezone:** all timestamps are stored as naive UTC (timezone info stripped on ingest) to keep comparisons consistent.
7. **DATA_DIR default:** `./data` (relative to the working directory where `uvicorn` is launched).

---

**🚀 Design & Performance Decisions**
**High-Speed Ingestion**: Utilized psycopg2's execute_values with a batch size of 5000 to minimize round-trips to the database. This allows the system to process a year's worth of data well within the 5-minute startup constraint.

**Query Optimization**: Implemented Functional Indexes (on DATE_TRUNC) and Composite Indexes (on product, status) to ensure the analytics endpoints remain responsive even as the dataset scales to millions of rows.

**Memory Efficiency**: Used Pandas with dtype=str and low_memory=False during ingestion to prevent type-inference overhead and memory spikes during the initial data load.

**Reliability**: Built an idempotent ingestion pipeline using ON CONFLICT DO NOTHING. If the server restarts or the ingestion is interrupted, it can resume without duplicating data or corrupting analytics.

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `psycopg2.OperationalError: could not connect` | Check `.env` credentials and that PostgreSQL is running |
| `No CSV files found` | Verify `DATA_DIR` path and file naming pattern `activities_YYYYMMDD.csv` |
| Port already in use | `lsof -i :8080` then kill the process, or change port with `--port 8081` |
| Slow startup | Normal for large datasets — PostgreSQL bulk insert is used for speed |
README
