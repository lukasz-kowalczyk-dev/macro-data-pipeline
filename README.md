# macro-data-pipeline

Fetches macroeconomic data from IMF and OECD APIs and loads it into Google BigQuery.
## Data

- **IMF** — annual: GDP growth, CPI inflation, current account balance, public debt, unemployment
- **OECD** — quarterly: GDP growth year-on-year

**Countries:** USA, Germany, UK, France, Japan, China, Poland, Switzerland, Sweden, Norway
<img width="1603" height="525" alt="1" src="https://github.com/user-attachments/assets/827a5dac-0434-46e8-839d-79f9533b08e8" />

## Commands

```bash
# Install
pip install -e ".[dev]"

# Run tests
python -m pytest tests/

# Dry run (no BigQuery write)
python -m pipeline.main --dry-run

# Full run
python -m pipeline.main
```

## Setup

1. `cp .env.example .env`
2. Fill in `GCP_PROJECT_ID`, `BQ_DATASET`, `GOOGLE_APPLICATION_CREDENTIALS`
3. Download service account key from GCP Console → IAM & Admin → Service Accounts → Keys
