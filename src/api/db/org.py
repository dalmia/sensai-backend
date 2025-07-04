from typing import Literal, List, Dict, Tuple
import secrets
import hashlib

from api.utils.db import (
    get_new_db_connection,
    execute_db_operation,
    execute_multiple_db_operations,
)
from api.config import (
    organizations_table_name,
    user_organizations_table_name,
    org_api_keys_table_name,
)
from api.db.user import get_user_by_id, insert_or_return_user
from api.slack import (
    send_slack_notification_for_new_org,
    send_slack_notification_for_member_added_to_org,
)


async def get_all_orgs() -> List[Dict]:
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(f"SELECT id, name, slug FROM {organizations_table_name}")

        return [
            {
                "id": row[0],
                "name": row[1],
                "slug": row[2],
            }
            for row in await cursor.fetchall()
        ]


def generate_api_key(org_id: int):
    """Generate a new API key"""
    # Create a random API key
    identifier = secrets.token_urlsafe(32)

    api_key = f"org__{org_id}__{identifier}"

    # Hash it for storage
    hashed_key = hashlib.sha256(api_key.encode()).hexdigest()
    return api_key, hashed_key  # Return both - give api_key to user, store hashed_key


async def create_org_api_key(org_id: int) -> str:
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        api_key, hashed_key = generate_api_key(org_id)

        await cursor.execute(
            f"INSERT INTO {org_api_keys_table_name} (org_id, hashed_key) VALUES (?, ?)",
            (org_id, hashed_key),
        )

        await conn.commit()

        return api_key


async def get_org_id_from_api_key(api_key: str) -> int:
    api_key_parts = api_key.split("__")

    if len(api_key_parts) != 3:
        raise ValueError("Invalid API key")

    try:
        org_id = int(api_key_parts[1])
    except ValueError:
        raise ValueError("Invalid API key")

    rows = await execute_db_operation(
        f"SELECT hashed_key FROM {org_api_keys_table_name} WHERE org_id = ?",
        (org_id,),
        fetch_all=True,
    )

    if not rows:
        raise ValueError("Invalid API key")

    hashed_key = hashlib.sha256(api_key.encode()).hexdigest()

    for row in rows:
        if hashed_key == row[0]:
            return org_id

    raise ValueError("Invalid API key")


async def create_organization_with_user(org_name: str, slug: str, user_id: int):
    user = await get_user_by_id(user_id)

    if not user:
        raise Exception(f"User with id '{user_id}' not found")

    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        await cursor.execute(
            f"SELECT id FROM {organizations_table_name} WHERE slug = ?",
            (slug,),
        )
        existing_org = await cursor.fetchone()

        if existing_org:
            raise Exception(f"Organization with slug '{slug}' already exists")

        await cursor.execute(
            f"""INSERT INTO {organizations_table_name} 
                (slug, name)
                VALUES (?, ?)""",
            (slug, org_name),
        )

        org_id = cursor.lastrowid
        await add_user_to_org_by_user_id(cursor, user_id, org_id, "owner")
        await conn.commit()

    await send_slack_notification_for_new_org(org_name, org_id, user)

    return org_id


def convert_org_db_to_dict(org: Tuple):
    if not org:
        return None

    return {
        "id": org[0],
        "slug": org[1],
        "name": org[2],
        "logo_color": org[3],
        "openai_api_key": org[5],
        "openai_free_trial": org[6],
    }


async def get_org_by_id(org_id: int):
    org_details = await execute_db_operation(
        f"SELECT * FROM {organizations_table_name} WHERE id = ?",
        (org_id,),
        fetch_one=True,
    )

    return convert_org_db_to_dict(org_details)


async def get_org_by_slug(slug: str):
    org_details = await execute_db_operation(
        f"SELECT * FROM {organizations_table_name} WHERE slug = ?",
        (slug,),
        fetch_one=True,
    )
    return convert_org_db_to_dict(org_details)


async def get_hva_org_id():
    hva_org_id = await execute_db_operation(
        "SELECT id FROM organizations WHERE name = ?",
        ("HyperVerge Academy",),
        fetch_one=True,
    )

    if hva_org_id is None:
        return None

    hva_org_id = hva_org_id[0]
    return hva_org_id


async def get_hva_cohort_ids() -> List[int]:
    hva_org_id = await get_hva_org_id()

    if hva_org_id is None:
        return []

    cohorts = await execute_db_operation(
        "SELECT id FROM cohorts WHERE org_id = ?",
        (hva_org_id,),
        fetch_all=True,
    )
    return [cohort[0] for cohort in cohorts]


async def is_user_hva_learner(user_id: int) -> bool:
    hva_cohort_ids = await get_hva_cohort_ids()

    if not hva_cohort_ids:
        return False

    num_hva_users_matching_user_id = (
        await execute_db_operation(
            f"SELECT COUNT(*) FROM user_cohorts WHERE user_id = ? AND cohort_id IN ({', '.join(map(str, hva_cohort_ids))}) AND role = 'learner'",
            (user_id,),
            fetch_one=True,
        )
    )[0]

    return num_hva_users_matching_user_id > 0


async def get_hva_openai_api_key() -> str:
    org_details = await get_org_by_id(await get_hva_org_id())
    return org_details["openai_api_key"]


async def add_users_to_org_by_email(
    org_id: int,
    emails: List[str],
):
    org = await get_org_by_id(org_id)

    if not org:
        raise Exception("Organization not found")

    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        user_ids = []
        for email in emails:
            user = await insert_or_return_user(cursor, email)
            user_ids.append(user["id"])

            await send_slack_notification_for_member_added_to_org(
                user, org["slug"], org_id
            )

        # Check if any of the users are already in the organization
        placeholders = ", ".join(["?" for _ in user_ids])

        await cursor.execute(
            f"""SELECT user_id FROM {user_organizations_table_name} 
            WHERE org_id = ? AND user_id IN ({placeholders})
            """,
            (org_id, *user_ids),
        )

        existing_user_ids = await cursor.fetchall()

        if existing_user_ids:
            raise Exception(f"Some users already exist in organization")

        await cursor.executemany(
            f"""INSERT INTO {user_organizations_table_name}
                (user_id, org_id, role)
                VALUES (?, ?, ?)""",
            [(user_id, org_id, "admin") for user_id in user_ids],
        )
        await conn.commit()


async def remove_members_from_org(org_id: int, user_ids: List[int]):
    query = f"DELETE FROM {user_organizations_table_name} WHERE org_id = ? AND user_id IN ({', '.join(map(str, user_ids))})"
    await execute_db_operation(query, (org_id,))


def convert_user_organization_db_to_dict(user_organization: Tuple):
    return {
        "id": user_organization[0],
        "user_id": user_organization[1],
        "org_id": user_organization[2],
        "role": user_organization[3],
    }


async def get_org_members(org_id: int):
    org_users = await execute_db_operation(
        f"""SELECT uo.user_id, u.email, uo.role 
        FROM {user_organizations_table_name} uo
        JOIN users u ON uo.user_id = u.id 
        WHERE uo.org_id = ?""",
        (org_id,),
        fetch_all=True,
    )

    return [
        {
            "id": org_user[0],
            "email": org_user[1],
            "role": org_user[2],
        }
        for org_user in org_users
    ]


def drop_user_organizations_table():
    execute_multiple_db_operations(
        [
            (f"DELETE FROM {user_organizations_table_name}", ()),
            (f"DROP TABLE IF EXISTS {user_organizations_table_name}", ()),
        ]
    )


def drop_organizations_table():
    drop_user_organizations_table()

    execute_multiple_db_operations(
        [
            (f"DELETE FROM {organizations_table_name}", ()),
            (f"DROP TABLE IF EXISTS {organizations_table_name}", ()),
        ]
    )


async def update_org(org_id: int, org_name: str):
    await execute_db_operation(
        f"UPDATE {organizations_table_name} SET name = ? WHERE id = ?",
        (org_name, org_id),
    )


async def update_org_openai_api_key(
    org_id: int, encrypted_openai_api_key: str, is_free_trial: bool
):
    await execute_db_operation(
        f"UPDATE {organizations_table_name} SET openai_api_key = ?, openai_free_trial = ? WHERE id = ?",
        (encrypted_openai_api_key, is_free_trial, org_id),
    )


async def clear_org_openai_api_key(org_id: int):
    await execute_db_operation(
        f"UPDATE {organizations_table_name} SET openai_api_key = NULL WHERE id = ?",
        (org_id,),
    )


async def add_user_to_org_by_user_id(
    cursor,
    user_id: int,
    org_id: int,
    role: Literal["owner", "admin"],
):
    await cursor.execute(
        f"""INSERT INTO {user_organizations_table_name}
            (user_id, org_id, role)
            VALUES (?, ?, ?)""",
        (user_id, org_id, role),
    )

    return cursor.lastrowid
