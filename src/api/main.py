import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.exceptions import RequestValidationError
import os
from os.path import exists
from api.config import UPLOAD_FOLDER_NAME
from api.routes import (
    auth,
    code,
    cohort,
    course,
    org,
    task,
    chat,
    user,
    milestone,
    hva,
    file,
    ai,
    scorecard,
)
from api.routes.ai import (
    resume_pending_task_generation_jobs,
    resume_pending_course_structure_generation_jobs,
)
from api.websockets import router as websocket_router
from api.scheduler import scheduler
from api.settings import settings
import bugsnag
from bugsnag.asgi import BugsnagMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.start()

    # Create the uploads directory if it doesn't exist
    os.makedirs(settings.local_upload_folder, exist_ok=True)

    # Add recovery logic for interrupted tasks
    asyncio.create_task(resume_pending_task_generation_jobs())
    asyncio.create_task(resume_pending_course_structure_generation_jobs())

    yield
    scheduler.shutdown()


if settings.bugsnag_api_key:
    bugsnag.configure(
        api_key=settings.bugsnag_api_key,
        project_root=os.path.dirname(os.path.abspath(__file__)),
        release_stage=settings.env or "development",
        notify_release_stages=["development", "staging", "production"],
        auto_capture_sessions=True,
    )


app = FastAPI(lifespan=lifespan)

# Add Bugsnag middleware if configured
if settings.bugsnag_api_key:
    app.add_middleware(BugsnagMiddleware)

    @app.middleware("http")
    async def bugsnag_request_middleware(request: Request, call_next):
        # Add request metadata to Bugsnag context
        bugsnag.configure_request(
            context=f"{request.method} {request.url.path}",
            request_data={
                "url": str(request.url),
                "method": request.method,
                "headers": dict(request.headers),
                "query_params": dict(request.query_params),
                "path_params": request.path_params,
                "client": {
                    "host": request.client.host if request.client else None,
                    "port": request.client.port if request.client else None,
                },
            },
        )

        response = await call_next(request)
        return response


# Add CORS middleware to allow cross-origin requests (for frontend to access backend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Replace with your frontend URL in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount the uploads folder as a static directory
if exists(settings.local_upload_folder):
    app.mount(
        f"/{UPLOAD_FOLDER_NAME}",
        StaticFiles(directory=settings.local_upload_folder),
        name="uploads",
    )

app.include_router(file.router, prefix="/file", tags=["file"])
app.include_router(ai.router, prefix="/ai", tags=["ai"])
app.include_router(auth.router, prefix="/auth", tags=["auth"])
app.include_router(task.router, prefix="/tasks", tags=["tasks"])
app.include_router(chat.router, prefix="/chat", tags=["chat"])
app.include_router(user.router, prefix="/users", tags=["users"])
app.include_router(org.router, prefix="/organizations", tags=["organizations"])
app.include_router(cohort.router, prefix="/cohorts", tags=["cohorts"])
app.include_router(course.router, prefix="/courses", tags=["courses"])
app.include_router(milestone.router, prefix="/milestones", tags=["milestones"])
app.include_router(scorecard.router, prefix="/scorecards", tags=["scorecards"])
app.include_router(code.router, prefix="/code", tags=["code"])
app.include_router(hva.router, prefix="/hva", tags=["hva"])
app.include_router(websocket_router, prefix="/ws", tags=["websockets"])


@app.get("/health")
async def health_check():
    return {"status": "ok"}
