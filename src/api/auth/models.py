from pydantic import BaseModel


class AuthenticatedUser(BaseModel):
    id: int
    email: str
    first_name: str | None = None
    last_name: str | None = None


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
