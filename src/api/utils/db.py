import sqlite3
from typing import List, Tuple
from api.config import sqlite_db_path
from api.utils.logging import logger
import aiosqlite
from contextlib import asynccontextmanager


def trace_callback(sql):
    # Record the start time and SQL
    logger.info(f"Executing operation: {sql}")


@asynccontextmanager
async def get_new_db_connection():
    conn = None
    try:
        conn = await aiosqlite.connect(sqlite_db_path)
        await conn.execute("PRAGMA synchronous=NORMAL;")
        await conn.set_trace_callback(trace_callback)
        yield conn
    except Exception as e:
        if conn:
            await conn.rollback()  # Rollback on any exception
        raise  # Re-raise the exception to propagate the error
    finally:
        if conn:
            await conn.close()


def set_db_defaults():
    conn = sqlite3.connect(sqlite_db_path)

    current_mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]

    if current_mode.lower() != "wal":
        settings = "PRAGMA journal_mode = WAL;"

        conn.executescript(settings)
        print("Defaults set.")
    else:
        print("Defaults already set.")


async def execute_db_operation(
    operation,
    params=None,
    fetch_one=False,
    fetch_all=False,
    get_last_row_id=False,
):
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        if params:
            await cursor.execute(operation, params)
        else:
            await cursor.execute(operation)

        if fetch_one:
            result = await cursor.fetchone()
        elif fetch_all:
            result = await cursor.fetchall()
        else:
            result = None

        await conn.commit()

        if get_last_row_id:
            return cursor.lastrowid

        return result


async def execute_many_db_operation(operation, params_list):
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.executemany(operation, params_list)
        await conn.commit()


async def execute_multiple_db_operations(commands_and_params: List[Tuple[str, Tuple]]):
    """
    Execute multiple SQL commands under the same connection.
    Each command is a tuple of (sql_command, params).
    All commands are executed in a single transaction.
    """
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        for command, params in commands_and_params:
            await cursor.execute(command, params)

        await conn.commit()


async def check_table_exists(table_name: str, cursor):
    await cursor.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    )
    table_exists = await cursor.fetchone()

    return table_exists is not None


def serialise_list_to_str(list_to_serialise: List[str]):
    if list_to_serialise:
        return ",".join(list_to_serialise)

    return None


def deserialise_list_from_str(str_to_deserialise: str):
    if str_to_deserialise:
        return str_to_deserialise.split(",")

    return []
