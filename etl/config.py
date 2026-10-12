"""PostgreSQL connection settings. Local dev container only (docker-compose.yml)."""
import os

import psycopg2


def connect():
    return psycopg2.connect(
        host=os.environ.get("GRID_PG_HOST", "localhost"),
        port=int(os.environ.get("GRID_PG_PORT", "5433")),
        dbname=os.environ.get("GRID_PG_DB", "grid_analytics"),
        user=os.environ.get("GRID_PG_USER", "grid_etl"),
        password=os.environ.get("GRID_PG_PASSWORD", "local_dev_only"),
    )
