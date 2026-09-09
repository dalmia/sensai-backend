# Authorization model

**Status:** enforcing (no log-only mode — the flag was removed by request)
**Last updated:** 2026-09-08

Three review rounds surfaced four bugs of the same shape: a predicate chosen from
the *route's* shape rather than from a role model (H1 learner-writes, M13
mentor-writes, H5 fail-open on an optional field, H7 soft-deleted membership).
This is the table that makes those obvious on inspection instead of on review.

## Roles

| Role | Stored as | Notes |
|---|---|---|
| **owner / admin** | `user_organizations.role` | "org staff" throughout |
| **mentor** | `user_cohorts.role = 'mentor'` | Cannot also be org staff — `add_members_to_cohort` refuses to add an org admin to a cohort |
| **learner** | `user_cohorts.role = 'learner'` | Reaches a course only via a cohort it is assigned to |
| **removed** | any of the above with `deleted_at` set | **Must behave as if the row does not exist** |

Everything resolves to an `org_id`: `courses`, `cohorts`, `tasks`, `scorecards`
and `milestones` carry it directly; `batches` via `cohort_id`; `questions` via
`task_id`.

## The matrix

Read down, not across — **a learner passes every read predicate**, which is why
no read predicate may ever guard a write.

| Resource | Read | Write |
|---|---|---|
| Course | staff · learner enrolled via cohort | **staff only** |
| Cohort | staff · any member | **staff only** |
| Task | staff · learner via course→cohort | **staff only** |
| Batch | staff · cohort member | **staff only** |
| Scorecard / Milestone | staff | staff |
| Org | any authenticated (id + slug + name + colour only) | staff |
| User record | self · org staff over them · **mentor** in a shared cohort | self · org staff (**no mentor**) |

Two deliberate exceptions, both learner-facing:

- `POST /tasks/{id}/complete` and `POST /ai/chat|assignment` use the **read**
  predicate — a learner acting on a task they can see is the intended flow.
  Identity comes from the token, so they can only act as themselves.
- `POST /cohorts/{id}/members` allows a **self-join**: own email only, exactly
  one role and it must be `learner`, and `org_slug` is **mandatory** — it is what
  makes this an invite rather than an open door.

## Two invariants

1. **Identity comes from the token, never the body.** Any route writing on behalf
   of a user reads `permissions.caller_id(request)`. A client-supplied `user_id`
   is never trusted on a self-scoped write.
2. **Removal revokes.** Every membership lookup filters `deleted_at IS NULL`.
   Without it, removing a member changes the UI and nothing else.

   Scoped to *membership* deliberately. The resource-existence lookups
   (`_org_of`, `cohort_for_batch`, `org_id_for_slug`) do **not** filter, and
   should not: a soft-deleted course still resolves an org, the write path
   still requires staff, and the learner read path is already closed because
   `delete_course` cascades the soft-delete to `course_cohorts`. Adding the
   filter there would change deletion semantics, not tighten access.

## Enforced by tests, not by memory

| Test | Property |
|---|---|
| `test_route_coverage.py::test_every_endpoint_is_guarded_or_explicitly_open` | No endpoint ships unguarded |
| `::test_mutations_never_use_a_read_predicate` | H1/M13 class — read predicates never guard writes |
| `::test_self_scoped_writes_take_identity_from_the_token` | Invariant 1 |
| `test_membership_revocation.py` | Invariant 2 — every predicate filters `deleted_at` |
| `test_authorization_routes.py` | Role-level: admin, learner **and mentor** |

The mentor client is the structural piece: H6 happened because no test had ever
authenticated as one.

## Removed rather than protected

- **`GET /organizations/`** returned **every organisation** to any authenticated
  caller. Nothing in the frontend called it (the only `organizations/` hit is the
  POST that creates one). Deleted, with its test.
- **`src/api/routes/integration.py`** — the router that leaked the Notion OAuth
  tokens. Already unregistered from `main.py`; deleted along with its test so it
  cannot be re-registered by accident.
- **`/docs`, `/redoc`, `/openapi.json`** — the attacker's first request in the
  2026-08-29 incident.
- **`/sentry-debug`** — any authenticated user could spam Sentry with it.

## Coverage

**84 of 93 endpoints guarded, 9 intentionally open** — `/auth/login`, `/health`
(×2), `/hva/org_id`, org by-id and by-slug, and the four `/file/*` routes.

The file routes are an **accepted risk**: any authenticated user who knows a file
UUID can fetch it. Closing it needs a file-ownership table that does not exist.
UUIDv4 is unguessable, so real but low-likelihood. Revisit if ownership records
are ever added.
