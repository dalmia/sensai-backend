import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
import os
from os.path import exists
from api.config import UPLOAD_FOLDER_NAME
from api.utils.logging import logger
from api.routes import (
    auth,
    batch,
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
    integration,
)

# from api.routes.ai import (
#     resume_pending_task_generation_jobs,
#     resume_pending_course_structure_generation_jobs,
# )
from api.websockets import router as websocket_router
from api.scheduler import scheduler
from api.settings import settings
import bugsnag
from bugsnag.asgi import BugsnagMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize comprehensive logging as the very first step
    logger.info("Starting application")

    scheduler.start()

    # Create the uploads directory if it doesn't exist
    os.makedirs(settings.local_upload_folder, exist_ok=True)

    # Add recovery logic for interrupted tasks
    # asyncio.create_task(resume_pending_task_generation_jobs())
    # asyncio.create_task(resume_pending_course_structure_generation_jobs())

    yield

    logger.info("Shutting down application")
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


# Add request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    # Log the incoming request
    logging.info(
        f"Incoming request: {request.method} {request.url.path} "
        f"from {request.client.host if request.client else 'unknown'}"
    )

    # Process the request
    start_time = asyncio.get_event_loop().time()
    try:
        response = await call_next(request)
        process_time = asyncio.get_event_loop().time() - start_time

        # Log the response
        logging.info(
            f"Request completed: {request.method} {request.url.path} "
            f"- Status: {response.status_code} - Duration: {process_time:.4f}s"
        )
        return response
    except Exception as e:
        process_time = asyncio.get_event_loop().time() - start_time
        logging.error(
            f"Error processing request: {request.method} {request.url.path} "
            f"- Error: {str(e)} - Duration: {process_time:.4f}s",
            exc_info=True,
        )
        raise


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
app.include_router(batch.router, prefix="/batches", tags=["batches"])
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
app.include_router(integration.router, prefix="/integrations", tags=["integrations"])


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logging.error(
        f"Unhandled exception: {type(exc).__name__}: {str(exc)} "
        f"on {request.method} {request.url.path}",
        exc_info=True,
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "An unexpected error occurred"},
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logging.warning(
        f"Validation error on {request.method} {request.url.path}: {exc.errors()}"
    )
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    if exc.status_code >= 500:
        logging.error(
            f"HTTP {exc.status_code} error on {request.method} {request.url.path}: {exc.detail}"
        )
    else:
        logging.info(
            f"HTTP {exc.status_code} on {request.method} {request.url.path}: {exc.detail}"
        )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
    )


@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check():
    return {"status": "ok"}


@app.api_route("/bugsnag-debug", methods=["GET"])
async def bugsnag_debug():
    bugsnag.notify(Exception("Test error"))
