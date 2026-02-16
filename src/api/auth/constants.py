ACCESS_TOKEN_EXPIRE_MINUTES = 60
JWT_ALGORITHM = "HS256"

# Role hierarchy: higher roles implicitly satisfy lower role checks.
# e.g. a check for "admin" also passes for "owner".
ROLE_HIERARCHY = {
    "owner": 4,
    "admin": 3,
    "mentor": 2,
    "learner": 1,
}
