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
                 "require_batch_access"}
    # Learner-facing mutations that legitimately use the read predicate.
    ALLOWED = {("POST", "/tasks/{task_id}/complete")}

    offenders = []
    for method, path, route in _api_routes():
        if method in ("GET", "HEAD") or (method, path) in ALLOWED:
            continue
        names = {getattr(d.call, "__name__", "") for d in route.dependant.dependencies}
        if names & READ_ONLY:
            offenders.append(f"{method} {path} -> {sorted(names & READ_ONLY)}")

    assert not offenders, "Mutations guarded by a learner-passing predicate:\n  " + "\n  ".join(offenders)
