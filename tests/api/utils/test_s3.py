import pytest
import os
import uuid
from unittest.mock import patch, MagicMock
from src.api.utils.s3 import (
    upload_file_to_s3,
    upload_audio_data_to_s3,
    download_file_from_s3_as_bytes,
    generate_s3_uuid,
    get_media_upload_s3_dir,
    get_media_upload_s3_key_from_uuid,
)


class TestS3Utils:
    @patch("src.api.utils.s3.boto3.Session")
    def test_upload_file_to_s3_success(self, mock_session):
        """Test successful file upload to S3."""
        # Setup mocks
        mock_s3_client = MagicMock()
        mock_session.return_value.client.return_value = mock_s3_client
        mock_s3_client.upload_file.return_value = None  # Successful upload returns None

        # Call the function
        result = upload_file_to_s3("/path/to/file.txt", "test/file.txt")

        # Check results
        assert result == "test/file.txt"
        mock_session.return_value.client.assert_called_once_with("s3")
        mock_s3_client.upload_file.assert_called_once()

    @patch("src.api.utils.s3.boto3.Session")
    def test_upload_file_to_s3_with_content_type(self, mock_session):
        """Test successful file upload to S3 with content type."""
        # Setup mocks
        mock_s3_client = MagicMock()
        mock_session.return_value.client.return_value = mock_s3_client
        mock_s3_client.upload_file.return_value = None  # Successful upload returns None

        # Call the function with content_type parameter
        result = upload_file_to_s3(
            "/path/to/file.json", "test/file.json", content_type="application/json"
        )

        # Check results
        assert result == "test/file.json"
        mock_session.return_value.client.assert_called_once_with("s3")

        # Verify upload_file was called with ExtraArgs containing ContentType
        call_args = mock_s3_client.upload_file.call_args
        assert call_args[1]["ExtraArgs"]["ContentType"] == "application/json"

    @patch("src.api.utils.s3.boto3.Session")
    def test_upload_file_to_s3_failure(self, mock_session):
        """Test file upload to S3 with an error."""
        # Setup mocks
        mock_s3_client = MagicMock()
        mock_session.return_value.client.return_value = mock_s3_client
        mock_s3_client.upload_file.return_value = {"Error": "Failed"}

        # Call the function and expect an exception
        with pytest.raises(Exception) as excinfo:
            upload_file_to_s3("/path/to/file.txt", "test/file.txt")

        # Check the exception message
        assert "Failed to upload to S3" in str(excinfo.value)

    @patch("src.api.utils.s3.boto3.Session")
    def test_upload_audio_data_to_s3_success(self, mock_session):
        """Test successful audio data upload to S3."""
        # Setup mocks
        mock_s3_client = MagicMock()
        mock_session.return_value.client.return_value = mock_s3_client
        mock_s3_client.put_object.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }

        # Call the function
        audio_data = b"test audio data"
        result = upload_audio_data_to_s3(audio_data, "test/audio.wav")

        # Check results
        assert result == "test/audio.wav"
        mock_session.return_value.client.assert_called_once_with("s3")
        mock_s3_client.put_object.assert_called_once()

    @patch("src.api.utils.s3.boto3.Session")
    def test_upload_audio_data_to_s3_invalid_extension(self, mock_session):
        """Test audio data upload with invalid file extension."""
        # Call the function with a non-WAV extension and expect an exception
        with pytest.raises(ValueError) as excinfo:
            upload_audio_data_to_s3(b"test audio data", "test/audio.mp3")

        # Check the exception message
        assert "Key must end with .wav extension" in str(excinfo.value)
        mock_session.return_value.client.assert_not_called()

    @patch("src.api.utils.s3.boto3.Session")
    def test_upload_audio_data_to_s3_failure(self, mock_session):
        """Test audio data upload to S3 with an error."""
        # Setup mocks
        mock_s3_client = MagicMock()
        mock_session.return_value.client.return_value = mock_s3_client
        mock_s3_client.put_object.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 500}
        }

        # Call the function and expect an exception
        with pytest.raises(Exception) as excinfo:
            upload_audio_data_to_s3(b"test audio data", "test/audio.wav")

        # Check the exception message
        assert "Failed to upload to S3" in str(excinfo.value)

    @patch("src.api.utils.s3.boto3.Session")
    def test_download_file_from_s3_as_bytes(self, mock_session):
        """Test downloading a file from S3 as bytes."""
        # Setup mocks
        mock_s3_client = MagicMock()
        mock_session.return_value.client.return_value = mock_s3_client
        mock_body = MagicMock()
        mock_body.read.return_value = b"file content"
        mock_s3_client.get_object.return_value = {"Body": mock_body}

        # Call the function
        result = download_file_from_s3_as_bytes("test/file.txt")

        # Check results
        assert result == b"file content"
        mock_session.return_value.client.assert_called_once_with("s3")
        mock_s3_client.get_object.assert_called_once()

    @patch("src.api.utils.s3.uuid.uuid4")
    def test_generate_s3_uuid(self, mock_uuid4):
        """Test generating a UUID for S3 keys."""
        # Setup mock
        mock_uuid4.return_value = uuid.UUID("12345678-1234-5678-1234-567812345678")

        # Call the function
        result = generate_s3_uuid()

        # Check results
        assert result == "12345678-1234-5678-1234-567812345678"
        mock_uuid4.assert_called_once()

    @patch("src.api.utils.s3.settings")
    @patch("src.api.utils.s3.join")
    def test_get_media_upload_s3_dir(self, mock_join, mock_settings):
        """Test getting the S3 directory for media uploads."""
        # Setup mocks
        mock_settings.s3_folder_name = "bucket-folder"
        mock_join.return_value = "bucket-folder/media"

        # Call the function
        result = get_media_upload_s3_dir()

        # Check results
        assert result == "bucket-folder/media"
        mock_join.assert_called_once_with("bucket-folder", "media")

    @patch("src.api.utils.s3.get_media_upload_s3_dir")
    @patch("src.api.utils.s3.join")
    def test_get_media_upload_s3_key_from_uuid(self, mock_join, mock_get_dir):
        """Test getting the S3 key for a media file using a UUID."""
        # Setup mocks
        mock_get_dir.return_value = "bucket-folder/media"
        mock_join.return_value = (
            "bucket-folder/media/12345678-1234-5678-1234-567812345678.jpg"
        )

        # Call the function
        result = get_media_upload_s3_key_from_uuid(
            "12345678-1234-5678-1234-567812345678", "jpg"
        )

        # Check results
        assert result == "bucket-folder/media/12345678-1234-5678-1234-567812345678.jpg"
        mock_get_dir.assert_called_once()
        mock_join.assert_called_once_with(
            "bucket-folder/media", "12345678-1234-5678-1234-567812345678.jpg"
        )
