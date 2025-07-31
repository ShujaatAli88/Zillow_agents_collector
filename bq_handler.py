from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from config import Config
from log_handler import logger


class BigQueryHandler:
    def __init__(self):
        self.project_id = Config.project_id
        self.dataset_id = Config.dataset_id
        self.credentials_path = Config.credentials_path
        self.full_table_id = f"{self.project_id}.{self.dataset_id}.{Config.table_id}"
        self.client = None

    def connect(self):
        logger.info("Connecting to BigQuery...")
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path
            )
            self.client = bigquery.Client(credentials=credentials, project=self.project_id)
            logger.info("✅ Connected successfully.")
        except Exception as error:
            logger.error(f"❌ Error connecting to BigQuery: {error}")

    def ensure_dataset_exists(self):
        logger.info(f"Checking if dataset '{self.dataset_id}' exists...")
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
            logger.info("✅ Dataset exists.")
        except Exception:
            logger.warning("⚠️ Dataset not found. Creating it...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Adjust if needed
            try:
                self.client.create_dataset(dataset)
                logger.info(f"✅ Dataset '{self.dataset_id}' created.")
            except Exception as error:
                logger.error(f"❌ Failed to create dataset: {error}")
                raise

    def sanitize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Sanitizing column names for BigQuery compatibility...")
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r"[^a-zA-Z0-9_]", "_", regex=True)
            .str.replace(r"_{2,}", "_", regex=True)
            .str.strip("_")
        )
        return df

    def insert_data(self, df: pd.DataFrame):
        logger.info("Uploading DataFrame to BigQuery...")
        
        if self.client is None:
            logger.error("❌ BigQuery client not initialized. Call connect() first.")
            return False

        try:
            self.ensure_dataset_exists()

            # Sanitize column names for BigQuery
            df = self.sanitize_column_names(df)

            # ✅ Convert all data to string to avoid schema type issues
            df = df.astype(str)

            # Optional: Replace known placeholders like 'N/A' or '-' with None if needed
            # df = df.replace({'N/A': None, '-': None})  # Uncomment if you prefer nulls

            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )

            job = self.client.load_table_from_dataframe(
                df, self.full_table_id, job_config=job_config
            )
            job.result()  # Wait for job to finish

            logger.info(f"✅ Inserted {job.output_rows} rows into {self.full_table_id}")
            return True

        except Exception as error:
            logger.exception(f"❌ Error inserting data into BigQuery: {error}")
            return False


# Main Script
if __name__ == "__main__":
    querydata_handler = BigQueryHandler()
    querydata_handler.connect()

    # Example dummy data
    dummy_data = {
        "Book Name": "Alice",
        "Book Price": "30",
        "Book Availability": "In Stock",
        "Book Description": "A tale of wonder",
        "Book Rating": "5"
    }

    df = pd.DataFrame([dummy_data])
    querydata_handler.insert_data(df)
