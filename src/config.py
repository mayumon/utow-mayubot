import os
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CHALLONGE_API_KEY = os.getenv("CHALLONGE_API_KEY")

if not DISCORD_TOKEN:
    raise RuntimeError("DISCORD_TOKEN is missing")

DB_PATH = os.environ.get("DB_PATH", "/data/utow.db")
