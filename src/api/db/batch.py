from typing import List, Dict, Optional
from api.config import (
    batches_table_name,
    user_batches_table_name,
    organizations_table_name,
    users_table_name,
    user_cohorts_table_name,
    cohorts_table_name,
)
from api.utils.db import (
    execute_db_operation,
    execute_multiple_db_operations,
    get_new_db_connection,
)
from api.db.user import insert_or_return_user


async def create_batch(name: str, cohort_id: int) -> int:
    """Create a new batch and return its ID"""
    return await execute_db_operation(
        f"""
        INSERT INTO {batches_table_name} (name, cohort_id)
        VALUES (?, ?)
        """,
        params=(name, cohort_id),
        get_last_row_id=True,
    )


async def create_batch_with_members(
    name: str, cohort_id: int, user_ids: List[int] = None
) -> int:
    """Create a new batch with optional initial members and return its ID"""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        # Create the batch first
        await cursor.execute(
            f"""
            INSERT INTO {batches_table_name} (name, cohort_id)
            VALUES (?, ?)
            """,
            (name, cohort_id),
        )
        batch_id = cursor.lastrowid

        # Add members if provided
        if user_ids and len(user_ids) > 0:
            values = [(user_id, batch_id) for user_id in user_ids]
            await cursor.executemany(
                f"""
                INSERT INTO {user_batches_table_name} (user_id, batch_id)
                VALUES (?, ?)
                """,
                values,
            )

        await conn.commit()
        return batch_id


async def delete_batch(batch_id: int):
    """Delete a batch and all its member associations"""
    await execute_multiple_db_operations(
        [
            (
                f"UPDATE {user_batches_table_name} SET deleted_at = CURRENT_TIMESTAMP WHERE batch_id = ? AND deleted_at IS NULL",
                (batch_id,),
            ),
            (
                f"UPDATE {batches_table_name} SET deleted_at = CURRENT_TIMESTAMP WHERE id = ? AND deleted_at IS NULL",
                (batch_id,),
            ),
        ]
    )


async def get_batches_for_user_in_cohort(user_id: int, cohort_id: int) -> List[Dict]:
    """
    Get all batches for a user that shares the same organization as the specified cohort.
    Returns batch name, batch id, and user's role in each batch.
    """
    # check if the cohort exists
    cohort = await execute_db_operation(
        f"""
        SELECT 1 FROM {cohorts_table_name} WHERE id = ?
        """,
        (cohort_id,),
        fetch_one=True,
    )

    if not cohort:
        raise Exception("Cohort not found")

    # First verify the user is in the specified cohort
    user_in_cohort = await execute_db_operation(
        f"""
        SELECT 1 FROM {user_cohorts_table_name} 
        WHERE user_id = ? AND cohort_id = ?
        """,
        (user_id, cohort_id),
        fetch_one=True,
    )

    if not user_in_cohort:
        raise Exception("User is not a member of the specified cohort")

    # Get all batches in the same organization where the user is a member
    results = await execute_db_operation(
        f"""
        SELECT b.id, b.name, uc.role
        FROM {batches_table_name} b
        JOIN {user_batches_table_name} ub ON b.id = ub.batch_id
        JOIN {user_cohorts_table_name} uc ON uc.user_id = ub.user_id AND uc.cohort_id = b.cohort_id
        WHERE ub.user_id = ? AND b.cohort_id = ?
        ORDER BY b.created_at DESC
        """,
        (user_id, cohort_id),
        fetch_all=True,
    )

    return [
        {
            "id": batch_id,
            "name": batch_name,
            "role": role,
        }
        for batch_id, batch_name, role in results
    ]


async def get_batch_by_id(batch_id: int) -> Dict:
    """Get batch details including all members"""
    # Fetch batch details
    batch = await execute_db_operation(
        f"""SELECT id, name, cohort_id FROM {batches_table_name} WHERE id = ?""",
        (batch_id,),
        fetch_one=True,
    )

    if not batch:
        return None

    # Get all users and their roles in the batch
    members = await execute_db_operation(
        f"""
        SELECT DISTINCT u.id, u.email, uc.role
        FROM {users_table_name} u
        JOIN {user_batches_table_name} ub ON u.id = ub.user_id 
        JOIN {user_cohorts_table_name} uc ON uc.user_id = ub.user_id AND uc.cohort_id = ?
        WHERE ub.batch_id = ?
        ORDER BY uc.role
        """,
        (batch[2], batch_id),
        fetch_all=True,
    )

    batch_data = {
        "id": batch[0],
        "name": batch[1],
        "cohort_id": batch[2],
        "members": [
            {"id": member[0], "email": member[1], "role": member[2]}
            for member in members
        ],
    }

    return batch_data


async def get_all_batches_for_cohort(cohort_id: int) -> List[Dict]:
    """Get all batches for a cohort with their members"""
    # Get all batches and their members in a single query
    results = await execute_db_operation(
        f"""
        SELECT 
            b.id as batch_id, 
            b.name as batch_name,
            u.id as user_id,
            u.email as user_email,
            uc.role as user_role
        FROM {batches_table_name} b
        LEFT JOIN {user_batches_table_name} ub ON b.id = ub.batch_id
        LEFT JOIN {users_table_name} u ON ub.user_id = u.id
        LEFT JOIN {user_cohorts_table_name} uc ON uc.user_id = ub.user_id AND uc.cohort_id = b.cohort_id
        WHERE b.cohort_id = ?
        ORDER BY b.id DESC, uc.role
        """,
        (cohort_id,),
        fetch_all=True,
    )

    # Group results by batch
    batches_dict = {}
    for row in results:
        batch_id, batch_name, user_id, user_email, user_role = row

        if batch_id not in batches_dict:
            batches_dict[batch_id] = {"id": batch_id, "name": batch_name, "members": []}

        # Add member if user data exists (LEFT JOIN might return NULL for batches without members)
        if user_id is not None:
            batches_dict[batch_id]["members"].append(
                {"id": user_id, "email": user_email, "role": user_role}
            )

    # Convert to list maintaining the order (ORDER BY b.id DESC)
    return list(batches_dict.values())


async def update_batch_name_and_members(
    batch_id: int,
    name: str,
    members_added: Optional[List[int]] = None,
    members_removed: Optional[List[int]] = None,
):
    """Update batch name and add/remove members in one transaction. No role in user_batches."""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()
        # Update name
        await cursor.execute(
            f"UPDATE {batches_table_name} SET name = ? WHERE id = ?",
            (name, batch_id),
        )
        # Add new members
        if members_added:
            # Check for existing users
            q = f"SELECT user_id FROM {user_batches_table_name} WHERE batch_id = ? AND user_id IN ({','.join(['?' for _ in members_added])})"
            await cursor.execute(q, (batch_id, *members_added))
            existing = await cursor.fetchall()
            if existing:
                raise Exception("One or more users are already in the batch")

            values = [(user_id, batch_id) for user_id in members_added]
            await cursor.executemany(
                f"INSERT INTO {user_batches_table_name} (user_id, batch_id) VALUES (?, ?)",
                values,
            )
        # Remove members
        if members_removed:
            q = f"SELECT user_id FROM {user_batches_table_name} WHERE batch_id = ? AND user_id IN ({','.join(['?' for _ in members_removed])})"
            await cursor.execute(q, (batch_id, *members_removed))
            found = await cursor.fetchall()

            if len(found) != len(members_removed):
                raise Exception("One or more members are not in the batch")

            await cursor.execute(
                f"UPDATE {user_batches_table_name} SET deleted_at = CURRENT_TIMESTAMP WHERE batch_id = ? AND user_id IN ({','.join(['?' for _ in members_removed])}) AND deleted_at IS NULL",
                (batch_id, *members_removed),
            )

        await conn.commit()

    return await get_batch_by_id(batch_id)


async def validate_batch_belongs_to_cohort(batch_id: int, cohort_id: int) -> bool:
    """
    Validate that a batch belongs to the specified cohort

    Args:
        batch_id: The ID of the batch to validate
        cohort_id: The ID of the cohort

    Returns:
        bool: True if the batch belongs to the cohort, False otherwise
    """
    result = await execute_db_operation(
        f"""
        SELECT 1 FROM {batches_table_name} 
        WHERE id = ? AND cohort_id = ?
        """,
        (batch_id, cohort_id),
        fetch_one=True,
    )

    return result is not None
