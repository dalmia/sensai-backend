import os
import re
import traceback
import uuid
from fastapi import APIRouter, HTTPException, File, UploadFile, Form
from fastapi.responses import FileResponse
from pydantic import BaseModel
import boto3
from botocore.exceptions import ClientError
from api.settings import settings
from api.utils.logging import logger
from api.utils.s3 import (
    generate_s3_uuid,
    get_media_upload_s3_key_from_uuid,
)
from api.models import (
    PresignedUrlRequest,
    PresignedUrlResponse,
    S3FetchPresignedUrlResponse,
)

router = APIRouter()
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_EXTENSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9-]{0,19}$")


@router.put("/presigned-url/create", response_model=PresignedUrlResponse)
async def get_upload_presigned_url(
    request: PresignedUrlRequest,
) -> PresignedUrlResponse:
    if not settings.s3_bucket_name:
        raise HTTPException(status_code=500, detail="S3 bucket name is not set")
    if not settings.s3_folder_name:
        raise HTTPException(status_code=500, detail="S3 folder name is not set")

    try:
        s3_client = boto3.client(
            "s3",
            region_name="ap-south-1",
            config=boto3.session.Config(signature_version="s3v4"),
        )

        uuid = generate_s3_uuid()
        key = get_media_upload_s3_key_from_uuid(
            uuid, request.content_type.split("/")[1]
        )

        presigned_url = s3_client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": settings.s3_bucket_name,
                "Key": key,
                "ContentType": request.content_type,
            },
            ExpiresIn=600,  # URL expires in 1 hour
        )

        return {
            "presigned_url": presigned_url,
            "file_key": key,
            "file_uuid": uuid,
        }

    except ClientError as e:
        logger.error(f"Error generating presigned URL: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to generate presigned URL")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An unexpected error occurred")


@router.get("/presigned-url/get")
async def get_download_presigned_url(
    uuid: str,
    file_extension: str,
) -> S3FetchPresignedUrlResponse:
    if not settings.s3_bucket_name:
        raise HTTPException(status_code=500, detail="S3 bucket name is not set")
    if not settings.s3_folder_name:
        raise HTTPException(status_code=500, detail="S3 folder name is not set")

    try:
        s3_client = boto3.client(
            "s3",
            region_name="ap-south-1",
            config=boto3.session.Config(signature_version="s3v4"),
        )

        key = get_media_upload_s3_key_from_uuid(uuid, file_extension)

        presigned_url = s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": settings.s3_bucket_name,
                "Key": key,
            },
            ExpiresIn=600,  # URL expires in 1 hour
        )

        return {"url": presigned_url}

    except ClientError as e:
        logger.error(f"Error generating download presigned URL: {str(e)}")
        raise HTTPException(
            status_code=500, detail="Failed to generate download presigned URL"
        )
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An unexpected error occurred")


@router.post("/upload-local")
async def upload_file_locally(
    file: UploadFile = File(...), content_type: str = Form(...)
):
    try:
        # Create the folder if it doesn't exist
        os.makedirs(settings.local_upload_folder, exist_ok=True)

        # Generate a unique filename
        file_uuid = str(uuid.uuid4())
        file_extension = content_type.split("/")[1]

        # Enforce the same shape the download route validates, so the two ends
        # cannot drift apart and leave an unreadable (or unsafe) filename.
        if not _EXTENSION_RE.match(file_extension):
            raise HTTPException(status_code=400, detail="Invalid content type")

        filename = f"{file_uuid}.{file_extension}"
        file_path = os.path.join(settings.local_upload_folder, filename)

        # Save the file
        contents = await file.read()
        with open(file_path, "wb") as f:
            f.write(contents)

        # Generate the URL to access the file statically
        static_url = f"/uploads/{filename}"

        return {
            "file_key": filename,
            "file_path": file_path,
            "file_uuid": file_uuid,
            "static_url": static_url,
        }

    except Exception as e:
        logger.error(f"Error uploading file locally: {str(e)}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Failed to upload file locally")


@router.get("/download-local/")
async def download_file_locally(
    uuid: str,
    file_extension: str,
):
    try:
        if not _UUID_RE.match(uuid):
            logger.warning(f"Rejected download, bad file identifier: {uuid!r}")
            raise HTTPException(status_code=400, detail="Invalid file identifier")

        if not _EXTENSION_RE.match(file_extension):
            logger.warning(f"Rejected download, bad extension: {file_extension!r}")
            raise HTTPException(status_code=400, detail="Invalid file extension")

        upload_root = os.path.realpath(settings.local_upload_folder)
        file_path = os.path.realpath(
            os.path.join(upload_root, f"{uuid}.{file_extension}")
        )

        # Defence in depth. Unreachable while the regexes above exclude "/", "\\"
        # and "." - kept so a symlink or a loosened regex cannot escape silently.
        if os.path.commonpath([upload_root, file_path]) != upload_root:
            logger.error(
                f"Path escaped upload folder: uuid={uuid!r} ext={file_extension!r}"
            )
            raise HTTPException(status_code=400, detail="Invalid file path")

        # Check if file exists
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="File not found")

        # Return the file as a response
        return FileResponse(
            path=file_path,
            filename=f"{uuid}.{file_extension}",
            media_type="application/octet-stream",
        )

    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error downloading file locally: {str(e)}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Failed to download file locally")
