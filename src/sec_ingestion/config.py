from pathlib import Path
import os

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parents[2]

# Load .env from the project root explicitly
load_dotenv(BASE_DIR / ".env")

DATA_DIR = BASE_DIR / "data"

BRONZE_DIR = DATA_DIR / "bronze" / "sec"
SILVER_DIR = DATA_DIR / "silver"


SEC_BASE_URL = "https://data.sec.gov"

SEC_USER_AGENT = os.getenv("SEC_USER_AGENT")