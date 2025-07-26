from typing import List, Optional, Dict, Any
from api.utils.db import get_new_db_connection
from api.config import integrations_table_name
from api.models import Integration, CreateIntegrationRequest, UpdateIntegrationRequest

async def create_integration(data: CreateIntegrationRequest) -> int:
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()
        # Check if record exists for user_id and integration_type
        await cursor.execute(
            f"SELECT id FROM {integrations_table_name} WHERE user_id = ? AND integration_type = ?",
            (data.user_id, data.integration_type)
        )
        row = await cursor.fetchone()
        if row:
            integration_id = row[0]
            # Update the existing record
            await cursor.execute(
                f"""
                UPDATE {integrations_table_name}
                SET access_token = ?, refresh_token = ?, expires_at = ?
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
            return integration_id
        # Insert new record
        await cursor.execute(
            f"""
            INSERT INTO {integrations_table_name} (user_id, integration_type, access_token, refresh_token, expires_at)
            VALUES (?, ?, ?, ?, ?)
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
        return cursor.lastrowid

async def get_integration(integration_id: int) -> Optional[Integration]:
    print(f"Getting integration {integration_id}")
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
        if user_id:
            await cursor.execute(
                f"SELECT id, user_id, integration_type, access_token, refresh_token, expires_at, created_at FROM {integrations_table_name} WHERE user_id = ?",
                (user_id,)
            )
        else:
            await cursor.execute(
                f"SELECT id, user_id, integration_type, access_token, refresh_token, expires_at, created_at FROM {integrations_table_name}"
            )
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