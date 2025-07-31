import os
from dotenv import load_dotenv

load_dotenv(override=True)
class Config:
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    dataset_id = os.getenv("DATASET_ID")
    table_id = os.getenv("TABLE_ID")
    project_id = os.getenv("PROJECT_ID")