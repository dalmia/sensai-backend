from fastapi import FastAPI

from api.middleware import permissions

TEST_USER_ID = 1

_PERMISSION_DEPENDENCIES = [
    getattr(permissions, name)
    for name in dir(permissions)
    if name.startswith("require_")
]


def bypass_permissions(app: FastAPI) -> FastAPI:
    """
    Stub out authorization on a test-local app.

    Some route tests build their own FastAPI instance with a single router and
    no auth middleware, so there is no authenticated caller for the permission
    dependencies to inspect.
    """
    for dependency in _PERMISSION_DEPENDENCIES:
        app.dependency_overrides[dependency] = lambda: None
    return app
