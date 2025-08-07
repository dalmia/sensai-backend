from typing import List, Optional, Dict, Any
from api.utils.db import get_new_db_connection
from api.config import integrations_table_name
from api.models import Integration, CreateIntegrationRequest, UpdateIntegrationRequest

async def create_integration(data: CreateIntegrationRequest) -> int:
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()
        await cursor.execute(
            f"""
            INSERT INTO {integrations_table_name} (user_id, integration_type, access_token, refresh_token, expires_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id, integration_type) DO UPDATE SET
                access_token = excluded.access_token,
                refresh_token = excluded.refresh_token,
                expires_at = excluded.expires_at
            """,
            (
                data.user_id,
                data.integration_type,
                data.access_token,
                data.refresh_token,
                data.expires_at,
            ),
        )
        await conn.commit()
        
        # Get the ID of the upserted record
        await cursor.execute(
            f"SELECT id FROM {integrations_table_name} WHERE user_id = ? AND integration_type = ?",
            (data.user_id, data.integration_type)
        )
        result = await cursor.fetchone()
        return result[0] if result else cursor.lastrowid

async def get_integration(integration_id: int) -> Optional[Integration]:
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()
        await cursor.execute(
            f"SELECT id, user_id, integration_type, access_token, refresh_token, expires_at, created_at FROM {integrations_table_name} WHERE id = ?",
            (integration_id,)
        )
        row = await cursor.fetchone()
        if row:
            return Integration(
                id=row[0], user_id=row[1], integration_type=row[2], access_token=row[3],
                refresh_token=row[4], expires_at=row[5], created_at=row[6]
            )
        return None

async def list_integrations(user_id: Optional[int] = None) -> List[Integration]:
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()
        
        query = f"SELECT id, user_id, integration_type, access_token, refresh_token, expires_at, created_at FROM {integrations_table_name}"
        params = ()
        
        if user_id:
            query += " WHERE user_id = ?"
            params = (user_id,)
            
        await cursor.execute(query, params)
        rows = await cursor.fetchall()
        return [
            Integration(
                id=row[0], user_id=row[1], integration_type=row[2], access_token=row[3],
                refresh_token=row[4], expires_at=row[5], created_at=row[6]
            ) for row in rows
        ]

async def update_integration(integration_id: int, data: UpdateIntegrationRequest) -> bool:
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()
        await cursor.execute(
            f"""
            UPDATE {integrations_table_name}
            SET access_token = COALESCE(?, access_token),
                refresh_token = COALESCE(?, refresh_token),
                expires_at = COALESCE(?, expires_at)
            WHERE id = ?
            """,
            (
                data.access_token,
                data.refresh_token,
                data.expires_at,
                integration_id,
            ),
        )
        await conn.commit()
        return cursor.rowcount > 0

async def delete_integration(integration_id: int) -> bool:
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()
        await cursor.execute(
            f"DELETE FROM {integrations_table_name} WHERE id = ?",
            (integration_id,)
        )
        await conn.commit()
        return cursor.rowcount > 0 