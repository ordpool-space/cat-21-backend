import os
import sys
import math
import time
import logging
import datetime
from typing import List
import psycopg2
from fastapi import FastAPI
from fastapi.exceptions import HTTPException
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
    catNumber: int
    blockHeight: int
    mintedAt: str
    mintedBy: str
    feeRate: float
    txHash: str

class Cat21PaginatedResult(BaseModel):
    cats: List[Cat]
    currentPage: int
    itemsPerPage: int
    totalResults: int
    totalPages: int


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
            LIMIT 100
            """
        )
        for item in cur.fetchall():
            cat = Cat(
                catNumber=item["cat_number"],
                blockHeight=item["block_height"],
                mintedAt=item["minted_at"].isoformat(),
                mintedBy=item["minted_by"],
                feeRate=item["feerate"],
                txHash=item["tx_hash"],
            )
            all_cats.append(cat)
            cat_by_number[cat.catNumber] = cat
            cat_by_minted_by[cat.mintedBy].append(cat)
        logging.info(f"Loaded {len(all_cats)} cats into in-memory cache")
        assert cat_by_number[0].mintedBy == "bc1p85ra9kv6a48yvk4mq4hx08wxk6t32tdjw9ylahergexkymsc3uwsdrx6sh"

def on_shutdown():
    pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Logic on startup
    on_startup()
    # Ready to process requests
    yield
    # Logic on shutdown
    on_shutdown()


# Initialize FastAPI app
app = FastAPI(
    title="CAT-21 Indexer API",
    description="Meow! Rescue the cats!",
    version="1.337",
    lifespan=lifespan
)


@app.get("/api/status", response_model=StatusResult)
async def get_status():
    uptime = int(time.time()) - STARTUP_TIME
    return StatusResult(
        network="mainnet",
        indexedCats=len(all_cats),
        lastSuccessfulExecution=all_cats[-1].mintedAt,
        uptime=uptime,
    )

# API endpoint /api/cats/{itemsPerPage}/{currentPage}
@app.get("/api/cats/{itemsPerPage}/{currentPage}", response_model=Cat21PaginatedResult)
async def get_cats(itemsPerPage: int = 10, currentPage: int = 1):
    # Input validation
    if currentPage < 1 or itemsPerPage < 1:
        raise HTTPException(404, {
            "statusCode": 404,
            "timestamp": datetime.datetime.now().isoformat(),
            "path": f"/api/cats/{itemsPerPage}/{currentPage}",
            "message": "Invalid input parameters"
            }
        )

    # Calculate how many pages we got
    total_pages = math.ceil(1.0 * len(all_cats) / itemsPerPage)

    # Check that we are within bounds
    if currentPage > total_pages:
        raise HTTPException(404, {
            "statusCode": 404,
            "timestamp": datetime.datetime.now().isoformat(),
            "path": f"/api/cats/{itemsPerPage}/{currentPage}",
            "message": "Pagination beyond end of list"
            }
        )

    # Calculate the offset based on currentPage that counts from 1
    offset = (currentPage - 1) * itemsPerPage

    # Get the cats from the in-memory cache
    cats = all_cats[offset : offset + itemsPerPage]
    return Cat21PaginatedResult(
        cats = cats,
        currentPage = currentPage,
        itemsPerPage = itemsPerPage,
        totalResults = len(all_cats),
        totalPages = total_pages,
    )
