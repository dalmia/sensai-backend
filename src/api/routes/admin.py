from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import Dict, List
import asyncio

from api.scheduler import (
    force_manual_bigquery_sync,
    get_bigquery_sync_status,
)
from api.bq.cron import sync_to_bigquery, sync_specific_tables

router = APIRouter()


@router.get("/bigquery/sync/status")
async def get_sync_status() -> Dict:
    """Get the status of BigQuery sync jobs."""
    return get_bigquery_sync_status()


@router.post("/bigquery/sync/manual")
async def trigger_manual_sync(background_tasks: BackgroundTasks) -> Dict:
    """Trigger a manual BigQuery sync."""
    try:
        force_manual_bigquery_sync()
        return {
            "success": True,
            "message": "Manual BigQuery sync triggered successfully",
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger sync: {str(e)}")


@router.post("/bigquery/sync/tables")
async def sync_specific_tables_endpoint(
    table_names: List[str], background_tasks: BackgroundTasks
) -> Dict:
    """Sync specific tables to BigQuery."""
    try:
        # Add the sync task to background tasks
        background_tasks.add_task(sync_specific_tables, table_names)
        return {
            "success": True,
            "message": f"Sync triggered for tables: {', '.join(table_names)}",
            "tables": table_names,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to trigger table sync: {str(e)}"
        )


@router.post("/bigquery/sync/full")
async def trigger_full_sync(background_tasks: BackgroundTasks) -> Dict:
    """Trigger a full BigQuery sync of all tables."""
    try:
        # Add the full sync task to background tasks
        background_tasks.add_task(sync_to_bigquery)
        return {"success": True, "message": "Full BigQuery sync triggered successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to trigger full sync: {str(e)}"
        )


@router.get("/bigquery/tables")
async def get_available_tables() -> Dict:
    """Get list of tables available for sync."""
    from api.bq.cron import BigQuerySyncer

    syncer = BigQuerySyncer()
    table_names = list(syncer.tables_to_sync.keys())

    return {"tables": table_names, "total_count": len(table_names)}
