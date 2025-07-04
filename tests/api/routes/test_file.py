import pytest
from fastapi import status
from unittest.mock import patch, MagicMock, mock_open, ANY
import boto3
from botocore.exceptions import ClientError
import os


@pytest.mark.asyncio
async def test_get_upload_presigned_url_success(client, mock_db):
    """
    Test getting a presigned URL for uploading a file successfully
    """
    with patch("api.routes.file.boto3.client") as mock_boto3_client, patch(
        "api.routes.file.generate_s3_uuid"
    ) as mock_generate_uuid, patch(
        "api.routes.file.settings.s3_folder_name", "test-folder"
    ), patch(
        "api.routes.file.settings.s3_bucket_name", "test-bucket"
    ):

        # Setup mocks
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_generate_uuid.return_value = "test-uuid"
        mock_s3.generate_presigned_url.return_value = (
            "https://presigned-url.example.com/upload"
        )

        request_body = {"content_type": "image/jpeg"}

        # Make request
        response = client.put("/file/presigned-url/create", json=request_body)

        # Assert response
        assert response.status_code == status.HTTP_200_OK
        response_json = response.json()
        assert (
            response_json["presigned_url"] == "https://presigned-url.example.com/upload"
        )
        assert response_json["file_key"] == "test-folder/media/test-uuid.jpeg"
        assert response_json["file_uuid"] == "test-uuid"

        # Assert mocks called correctly
        mock_boto3_client.assert_called_with(
            "s3",
            region_name="ap-south-1",
            config=ANY,
        )
        mock_s3.generate_presigned_url.assert_called_with(
            "put_object",
            Params={
                "Bucket": "test-bucket",
                "Key": "test-folder/media/test-uuid.jpeg",
                "ContentType": "image/jpeg",
            },
            ExpiresIn=600,
        )


@pytest.mark.asyncio
async def test_get_upload_presigned_url_s3_folder_not_set(client, mock_db):
    """
    Test getting a presigned URL when S3 folder name is not set
    """
    with patch("api.routes.file.settings.s3_folder_name", None):
        request_body = {"content_type": "image/jpeg"}

        # Make request
        response = client.put("/file/presigned-url/create", json=request_body)

        # Assert response
        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert response.json() == {"detail": "S3 folder name is not set"}


@pytest.mark.asyncio
async def test_get_upload_presigned_url_client_error(client, mock_db):
    """
    Test getting a presigned URL when boto3 client raises an error
    """
    with patch("api.routes.file.boto3.client") as mock_boto3_client, patch(
        "api.routes.file.settings.s3_folder_name", "test-folder"
    ), patch("api.routes.file.settings.s3_bucket_name", "test-bucket"):

        # Setup mocks to raise error
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.generate_presigned_url.side_effect = ClientError(
            {"Error": {"Code": "SomeError", "Message": "Some error message"}},
            "generate_presigned_url",
        )

        request_body = {"content_type": "image/jpeg"}

        # Make request
        response = client.put("/file/presigned-url/create", json=request_body)

        # Assert response
        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert response.json() == {"detail": "Failed to generate presigned URL"}


@pytest.mark.asyncio
async def test_get_upload_presigned_url_unexpected_error(client, mock_db):
    """
    Test getting a presigned URL when an unexpected error occurs
    """
    with patch("api.routes.file.boto3.client") as mock_boto3_client, patch(
        "api.routes.file.settings.s3_folder_name", "test-folder"
    ), patch("api.routes.file.settings.s3_bucket_name", "test-bucket"), patch(
        "api.routes.file.traceback.print_exc"
    ) as mock_traceback:

        # Setup mocks to raise unexpected error
        mock_boto3_client.side_effect = ValueError("Unexpected error")

        request_body = {"content_type": "image/jpeg"}

        # Make request
        response = client.put("/file/presigned-url/create", json=request_body)

        # Assert response
        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert response.json() == {"detail": "An unexpected error occurred"}
        mock_traceback.assert_called_once()


@pytest.mark.asyncio
async def test_get_download_presigned_url_success(client, mock_db):
    """
    Test getting a presigned URL for downloading a file successfully
    """
    with patch("api.routes.file.boto3.client") as mock_boto3_client, patch(
        "api.routes.file.settings.s3_folder_name", "test-folder"
    ), patch("api.routes.file.settings.s3_bucket_name", "test-bucket"):

        # Setup mocks
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.generate_presigned_url.return_value = (
            "https://presigned-url.example.com/download"
        )

        uuid = "test-uuid"
        file_extension = "jpeg"

        # Make request
        response = client.get(
            f"/file/presigned-url/get?uuid={uuid}&file_extension={file_extension}"
        )

        # Assert response
        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"url": "https://presigned-url.example.com/download"}

        # Assert mocks called correctly
        mock_boto3_client.assert_called_with(
            "s3",
            region_name="ap-south-1",
            config=ANY,
        )
        mock_s3.generate_presigned_url.assert_called_with(
            "get_object",
            Params={
                "Bucket": "test-bucket",
                "Key": f"test-folder/media/{uuid}.{file_extension}",
            },
            ExpiresIn=600,
        )


@pytest.mark.asyncio
async def test_get_download_presigned_url_s3_folder_not_set(client, mock_db):
    """
    Test getting a download presigned URL when S3 folder name is not set
    """
    with patch("api.routes.file.settings.s3_folder_name", None):
        uuid = "test-uuid"
        file_extension = "jpeg"

        # Make request
        response = client.get(
            f"/file/presigned-url/get?uuid={uuid}&file_extension={file_extension}"
        )

        # Assert response
        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert response.json() == {"detail": "S3 folder name is not set"}


@pytest.mark.asyncio
async def test_get_download_presigned_url_client_error(client, mock_db):
    """
    Test getting a download presigned URL when boto3 client raises an error
    """
    with patch("api.routes.file.boto3.client") as mock_boto3_client, patch(
        "api.routes.file.settings.s3_folder_name", "test-folder"
    ), patch("api.routes.file.settings.s3_bucket_name", "test-bucket"):

        # Setup mocks to raise error
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.generate_presigned_url.side_effect = ClientError(
            {"Error": {"Code": "SomeError", "Message": "Some error message"}},
            "generate_presigned_url",
        )

        uuid = "test-uuid"
        file_extension = "jpeg"

        # Make request
        response = client.get(
            f"/file/presigned-url/get?uuid={uuid}&file_extension={file_extension}"
        )

        # Assert response
        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert response.json() == {
            "detail": "Failed to generate download presigned URL"
        }


@pytest.mark.asyncio
async def test_get_download_presigned_url_unexpected_error(client, mock_db):
    """
    Test getting a download presigned URL when an unexpected error occurs
    """
    with patch("api.routes.file.boto3.client") as mock_boto3_client, patch(
        "api.routes.file.settings.s3_folder_name", "test-folder"
    ), patch("api.routes.file.settings.s3_bucket_name", "test-bucket"), patch(
        "api.routes.file.traceback.print_exc"
    ) as mock_traceback:

        # Setup mocks to raise unexpected error
        mock_boto3_client.side_effect = RuntimeError("Unexpected runtime error")

        uuid = "test-uuid"
        file_extension = "jpeg"

        # Make request
        response = client.get(
            f"/file/presigned-url/get?uuid={uuid}&file_extension={file_extension}"
        )

        # Assert response
        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert response.json() == {"detail": "An unexpected error occurred"}
        mock_traceback.assert_called_once()


@pytest.mark.asyncio
async def test_upload_file_locally_success(client, mock_db):
    """
    Test uploading a file locally successfully
    """
    with patch("api.routes.file.os.makedirs") as mock_makedirs, patch(
        "api.routes.file.open", mock_open()
    ) as mock_file, patch("api.routes.file.uuid.uuid4") as mock_uuid, patch(
        "api.routes.file.settings.local_upload_folder", "/tmp/uploads"
    ):

        # Setup mocks
        mock_uuid.return_value = "test-uuid"

        # Make request with multipart form data
        response = client.post(
            "/file/upload-local",
            files={"file": ("test.jpg", b"test content", "image/jpeg")},
            data={"content_type": "image/jpeg"},
        )

        # Assert response
        assert response.status_code == status.HTTP_200_OK
        result = response.json()
        assert result["file_key"] == "test-uuid.jpeg"
        assert result["file_path"] == "/tmp/uploads/test-uuid.jpeg"
        assert result["file_uuid"] == "test-uuid"
        assert result["static_url"] == "/uploads/test-uuid.jpeg"

        # Assert mocks called correctly
        mock_makedirs.assert_called_with("/tmp/uploads", exist_ok=True)
        mock_file.assert_called_with("/tmp/uploads/test-uuid.jpeg", "wb")
        mock_file().write.assert_called_once()


@pytest.mark.asyncio
async def test_upload_file_locally_error(client, mock_db):
    """
    Test uploading a file locally with an error
    """
    with patch("api.routes.file.os.makedirs") as mock_makedirs, patch(
        "api.routes.file.traceback.print_exc"
    ) as mock_traceback:
        # Setup mocks to raise error
        mock_makedirs.side_effect = Exception("File system error")

        # Make request with multipart form data
        response = client.post(
            "/file/upload-local",
            files={"file": ("test.jpg", b"test content", "image/jpeg")},
            data={"content_type": "image/jpeg"},
        )

        # Assert response
        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert response.json() == {"detail": "Failed to upload file locally"}
        mock_traceback.assert_called_once()


@pytest.mark.asyncio
async def test_download_file_locally_success(client, mock_db):
    """
    Test downloading a file locally successfully
    """
    with patch("api.routes.file.os.path.exists") as mock_exists, patch(
        "api.routes.file.FileResponse"
    ) as mock_file_response, patch(
        "api.routes.file.settings.local_upload_folder", "/tmp/uploads"
    ):

        # Setup mocks
        mock_exists.return_value = True
        mock_file_response.return_value = "file_response"  # Simplified for testing

        uuid = "test-uuid"
        file_extension = "jpeg"

        # Make request
        response = client.get(
            f"/file/download-local/?uuid={uuid}&file_extension={file_extension}"
        )

        # Assert mocks called correctly
        mock_exists.assert_called_with(f"/tmp/uploads/{uuid}.{file_extension}")
        mock_file_response.assert_called_with(
            path=f"/tmp/uploads/{uuid}.{file_extension}",
            filename=f"{uuid}.{file_extension}",
            media_type="application/octet-stream",
        )


@pytest.mark.asyncio
async def test_download_file_locally_file_not_found(client, mock_db):
    """
    Test downloading a file locally when the file doesn't exist
    """
    with patch("api.routes.file.os.path.exists") as mock_exists, patch(
        "api.routes.file.settings.local_upload_folder", "/tmp/uploads"
    ):

        # Setup mocks
        mock_exists.return_value = False

        uuid = "test-uuid"
        file_extension = "jpeg"

        # Make request
        response = client.get(
            f"/file/download-local/?uuid={uuid}&file_extension={file_extension}"
        )

        # Assert response
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert response.json() == {"detail": "File not found"}


@pytest.mark.asyncio
async def test_download_file_locally_unexpected_error(client, mock_db):
    """
    Test downloading a file locally when an unexpected error occurs
    """
    with patch("api.routes.file.os.path.exists") as mock_exists, patch(
        "api.routes.file.settings.local_upload_folder", "/tmp/uploads"
    ), patch("api.routes.file.traceback.print_exc") as mock_traceback:

        # Setup mocks to raise unexpected error
        mock_exists.side_effect = RuntimeError("Unexpected file system error")

        uuid = "test-uuid"
        file_extension = "jpeg"

        # Make request
        response = client.get(
            f"/file/download-local/?uuid={uuid}&file_extension={file_extension}"
        )

        # Assert response
        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert response.json() == {"detail": "Failed to download file locally"}
        mock_traceback.assert_called_once()
