from fastapi import APIRouter, Depends, HTTPException
from typing import List, Dict
from api.db.user import insert_or_return_user
from api.utils.db import get_new_db_connection
from api.models import UserLoginData
from google.oauth2 import id_token
from google.auth.transport import requests
from api.settings import settings
import os

router = APIRouter()


@router.post("/login")
async def login_or_signup_user(user_data: UserLoginData) -> Dict:
    # Verify the Google ID token
    try:
        # Get Google Client ID from environment variable
        if not settings.google_client_id:
            raise HTTPException(
                status_code=500, detail="Google Client ID not configured"
            )

        # Verify the token with Google
        id_info = id_token.verify_oauth2_token(
            user_data.id_token, requests.Request(), settings.google_client_id
        )

        # Check that the email in the token matches the provided email
        if id_info["email"] != user_data.email:
            raise HTTPException(
                status_code=401, detail="Email in token doesn't match provided email"
            )

    except ValueError as e:
        # Invalid token
        raise HTTPException(
            status_code=401, detail=f"Invalid authentication token: {str(e)}"
        )

    # If token is valid, proceed with user creation/retrieval
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()
        user = await insert_or_return_user(
            cursor,
            user_data.email,
            user_data.given_name,
            user_data.family_name,
        )
        await conn.commit()

    return user
