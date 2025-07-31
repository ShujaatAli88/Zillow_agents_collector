# Zillow_agents_collector# Zillow Real Estate Agents Email Collector

This project scrapes real estate agent data from Zillow, extracts detailed information, and uploads it to Google BigQuery for further analysis.

## Features

- Scrapes agent listings and detail pages from Zillow.
- Extracts agent name, email, phone, business info, sales stats, and more.
- Validates and structures data using Pydantic models.
- Saves data locally as CSV and uploads to Google BigQuery.
- Uses rotating proxies and robust logging.

## Project Structure

- `main.py` — Entry point for running the scraper.
- [`agents_collector.py`](agents_collector.py) — Main scraping logic and BigQuery integration.
- [`zillow_agents_crawler.py`](zillow_agents_crawler.py) — Alternative/legacy scraper logic.
- [`bq_handler.py`](bq_handler.py) — Handles BigQuery authentication and data upload.
- [`models.py`](models.py) — Pydantic data models for validation.
- [`constants.py`](constants.py) — HTTP headers, cookies, and other constants.
- [`proxy_handler.py`](proxy_handler.py) — Proxy management.
- [`log_handler.py`](log_handler.py) — Logging setup using Loguru.
- `requirements.txt` — Python dependencies.
- `.env` — Environment variables for credentials and configuration.

## Setup

1. **Clone the repository** and navigate to the project directory.

2. **Install dependencies:**
   ```sh
   pip install -r requirements.txt
   ```

3. **Configure environment variables:**
   - Copy `.env` and update with your Google Cloud project, dataset, table, and service account JSON path.

4. **Place your Google Cloud service account key** (JSON) in the project root.

## Usage

Run the main scraper:
```sh
python main.py
```

Logs will be saved in the `logs/` directory. Scraped data is uploaded to BigQuery and optionally saved as `zillow_agents_data.csv`.

## Customization

- **Target Location:** Change the city/URL in [`agents_collector.py`](agents_collector.py) as needed.
- **BigQuery Table:** Update `.env` for different datasets/tables.
- **Proxy Settings:** Edit [`proxy_handler.py`](proxy_handler.py) to use your own proxies.

## Notes

- Use responsibly and respect Zillow's terms of service.
- For large-scale scraping, consider rate limits and proxy rotation.