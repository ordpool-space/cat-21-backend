import os
import sys
import time
import logging
import datetime
import psycopg2
from fastapi import FastAPI
from pydantic import BaseModel
from dotenv import load_dotenv
from urllib.parse import quote_plus
from contextlib import asynccontextmanager
from psycopg2.extras import DictCursor
from collections import defaultdict

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
    stream=sys.stdout,
)

STARTUP_TIME = int(time.time())


# Pydantic models
class StatusResult(BaseModel):
    network: str
    indexedCats: int
    lastSuccessfulExecution: str
    uptime: int


class Cat(BaseModel):
    cat_number: int
    block_height: int
    minted_at: datetime.datetime
    minted_by: str
    feerate: float
    tx_hash: str


def get_db_connection():
    """Connect to PostgreSQL database and return connection object"""
    # Define the additional option with URL encoding
    encoded_option = quote_plus(f"endpoint={os.getenv('DATABASE_ENDPOINT')}")
    # Connect using psycopg2 with connection string
    connection_string = f"postgresql://{os.getenv('DATABASE_USER')}:{os.getenv('DATABASE_PASSWORD')}@{os.getenv('DATABASE_HOST')}:{os.getenv('DATABASE_PORT')}/{os.getenv('DATABASE_NAME')}?options={encoded_option}"
    return psycopg2.connect(connection_string)


# In-memory caches
all_cats = []
cat_by_number = defaultdict(Cat)
cat_by_minted_by = defaultdict(list)


def on_startup():
    conn = get_db_connection()
    with conn.cursor(cursor_factory=DictCursor) as cur:
        # Test that database connection is OK
        cur.execute("SELECT NOW();")
        current_time = cur.fetchone()
        logging.info(
            f"Connected to database with server time {current_time[0].strftime('%Y-%m-%d %H:%M')} UTC"
        )
        
        # Load dataset into memory
        cur.execute(
            """
            SELECT cat_number, block_height, minted_at, minted_by, feerate, tx_hash
            FROM cats
            ORDER BY cat_number ASC
            LIMIT 10
            """
        )
        for item in cur.fetchall():
            cat = Cat(
                cat_number=item["cat_number"],
                block_height=item["block_height"],
                minted_at=item["minted_at"],
                minted_by=item["minted_by"],
                feerate=item["feerate"],
                tx_hash=item["tx_hash"],
            )
            all_cats.append(cat)
            cat_by_number[cat.cat_number] = cat
            cat_by_minted_by[cat.minted_by].append(cat)
        logging.info(f"Loaded {len(all_cats)} cats into in-memory cache")
        assert cat_by_number[0].minted_by == "bc1p85ra9kv6a48yvk4mq4hx08wxk6t32tdjw9ylahergexkymsc3uwsdrx6sh"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Logic on startup
    on_startup()
    
    # Ready to process requests
    yield
    
    # Logic on shutdown
    # ...


# Initialize FastAPI app
app = FastAPI(
    title="CAT-21 Indexer API",
    description="Meow! Rescue the cats!",
    version="2.0",
    lifespan=lifespan
)


@app.get("/api/status", response_model=StatusResult)
async def get_status():
    uptime = int(time.time()) - STARTUP_TIME

    return StatusResult(
        network="mainnet",
        indexedCats=len(all_cats),
        lastSuccessfulExecution=all_cats[-1].minted_at.isoformat(),
        uptime=uptime,
    )
