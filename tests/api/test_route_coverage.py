"""
Guard-rail: every endpoint must be guarded or explicitly listed as open.

Three endpoints shipped unguarded in the first pass because their decorators
spanned multiple lines. This turns "did we miss one?" into a failing build.
"""

import inspect

from api.main import app

# Endpoints that are intentionally reachable by any authenticated caller.
# Adding to this list should be a deliberate, reviewed decision.
INTENTIONALLY_OPEN = {
    ("POST", "/auth/login"),
    ("GET", "/health"),
    ("HEAD", "/health"),
    ("GET", "/hva/org_id"),
    ("GET", "/organizations/slug/{slug}"),
    ("GET", "/organizations/{org_id}"),
    ("PUT", "/file/presigned-url/create"),
    ("GET", "/file/presigned-url/get"),
    ("POST", "/file/upload-local"),
    ("GET", "/file/download-local/"),
}


def _guarded_by_dependency(route) -> bool:
    return any(
        getattr(dep.call, "__name__", "").startswith("require_")
        for dep in route.dependant.dependencies
    )


def _guarded_in_body(route) -> bool:
    try:
        source = inspect.getsource(route.endpoint)
    except (OSError, TypeError):
        return False
    return "permissions.require_" in source or "permissions.caller_id" in source


def _api_routes():
    for route in app.routes:
        methods = getattr(route, "methods", None)
        if not methods or not hasattr(route, "dependant"):
            continue
        for method in methods:
            if method == "OPTIONS":
                continue
            yield method, route.path, route


def test_every_endpoint_is_guarded_or_explicitly_open():
    unguarded = [
        f"{method} {path}"
        for method, path, route in _api_routes()
        if (method, path) not in INTENTIONALLY_OPEN
        and not _guarded_by_dependency(route)
        and not _guarded_in_body(route)
    ]

    assert not unguarded, (
        "These endpoints have no authorization and are not on the "
        f"intentionally-open list:\n  " + "\n  ".join(sorted(unguarded))
    )


def test_open_list_has_no_stale_entries():
    live = {(method, path) for method, path, _ in _api_routes()}
    stale = INTENTIONALLY_OPEN - live
    assert not stale, f"Intentionally-open list references routes that no longer exist: {stale}"


def test_mutations_never_use_a_read_predicate():
    """
    A learner passes the read predicates, so they must never guard a mutation.
    This is the H1 finding: DELETE /courses/{id} was reachable by any enrolled
    learner because it reused can_access_course.
    """
    READ_ONLY = {"require_course_access", "require_cohort_access", "require_task_access",
                 "require_batch_access", "require_user_scope"}
    # A learner completing a task they can see is the intended flow, so the read
    # predicate is right here. Who gets marked complete comes from the token,
    # not the body - see mark_task_completed.
    ALLOWED = {("POST", "/tasks/{task_id}/complete")}

    offenders = []
    for method, path, route in _api_routes():
        if method in ("GET", "HEAD") or (method, path) in ALLOWED:
            continue
        names = {getattr(d.call, "__name__", "") for d in route.dependant.dependencies}
        if names & READ_ONLY:
            offenders.append(f"{method} {path} -> {sorted(names & READ_ONLY)}")

    assert not offenders, "Mutations guarded by a learner-passing predicate:\n  " + "\n  ".join(offenders)


def test_self_scoped_writes_take_identity_from_the_token():
    """
    A route that writes on behalf of a user must not read user_id from the body.
    /tasks/{id}/complete shipped that way and let a learner mark someone else's
    task complete, which feeds streaks, leaderboards and the BigQuery sync.
    """
    import inspect

    SELF_SCOPED_WRITES = [
        ("POST", "/tasks/{task_id}/complete"),
        ("POST", "/chat/"),
        ("POST", "/code/"),
        ("POST", "/ai/chat"),
        ("POST", "/ai/assignment"),
        ("POST", "/organizations/"),
    ]

    offenders = []
    for method, path, route in _api_routes():
        if (method, path) not in SELF_SCOPED_WRITES:
            continue
        source = inspect.getsource(route.endpoint)
        if "permissions.caller_id(http_request)" not in source:
            offenders.append(f"{method} {path}")

    assert not offenders, (
        "These writes still trust a client-supplied user_id:\n  " + "\n  ".join(offenders)
    )
