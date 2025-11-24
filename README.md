# KOSPI Sector Trend Analysis App

## Overview

This project provides an interactive Streamlit dashboard for analyzing market capitalization trends across KOSPI sectors. Users can explore sector-level changes over time, review top-performing sectors, analyze disparity metrics, and download filtered datasets. All data is sourced from the Korea Exchange (KRX) and refreshed through automated crawling.

**Live Dashboard:**

https://kospi-sector-trend-lhxhn2hk4qlgkajh6ubgfu.streamlit.app/

## Data Source

- **Provider:** Korea Exchange (KRX)
- **URL:** https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020505
- **Files Generated (via crawling):**
    - `KRX_sector_mktcap.csv` — sector-level historical market capitalization
    - `KRX_sector_company.csv` — sector–company mapping
- Data is collected through custom crawling scripts and updated regularly via GitHub Actions.

## Features

- Date range filtering with trading-day validation
- Aggregation by year, month, week, or day
- Sector-level filtering including full market view
- End-to-end market cap change calculation
- Top 5 sectors by percentage change
- Treemap visualization of sector-level performance
- Sector-specific market cap and disparity analysis
- Selection of N-day (20/50/100/150/200) disparity metrics
- Export filtered data to Excel
- Link to official KRX data portal

## Technologies Used

- **Frontend & Dashboard:** Streamlit, Plotly Express
- **Data Processing:** pandas, numpy
- **Exports:** openpyxl
- **Automation:** GitHub Actions (CI/CD for crawling + application updates)

## Pipeline Summary

1. GitHub Actions runs daily at 6 PM, triggering the automated crawling workflow.
2. Crawling scripts retrieve:
    - Sector-level market capitalization
    - Sector–company mapping
3. Processed data is saved as CSV files under the `data/` directory.
4. Streamlit loads these files and renders:
    - Market cap trends
    - Sector rankings
    - Treemap visualizations
    - Disparity charts
5. Users interactively filter results, explore trends, and download data as Excel files.

## How to Run Locally

```bash
git clone https://github.com/son-seongyeon/Kospi-Sector-Trend.git
cd Kospi-Sector-Trend

python -m venv venv
source venv/bin/activate     # Windows: venv\\Scripts\\activate

pip install -r requirements.txt
streamlit run app.py
```

## Future Improvements

- Add KOSDAQ and additional market support
- Introduce English-language UI toggle
- Expand technical indicators and chart types
- Transition to database storage for scalable historical data
