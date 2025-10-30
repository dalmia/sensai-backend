from api.config import UPLOAD_FOLDER_NAME
import pytest
import os
import uuid
from unittest.mock import patch, MagicMock
from src.api.utils.s3 import (
    get_uuid_from_url,
    upload_file_to_s3,
    upload_audio_data_to_s3,
    download_file_from_s3_as_bytes,
    generate_s3_uuid,
    get_media_upload_s3_dir,
    get_media_upload_s3_key_from_uuid
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

    @patch("src.api.utils.s3.get_media_upload_s3_dir")
    @patch("src.api.utils.s3.join")
    def test_get_media_upload_s3_key_from_uuid_empty_extension(self, mock_join, mock_get_dir):
        """Test getting the S3 key for a media file using a UUID when extension is empty."""
        # Setup mocks
        mock_get_dir.return_value = "bucket-folder/media"
        mock_join.return_value = (
            "bucket-folder/media/12345678-1234-5678-1234-567812345678.jpg"
        )

        # Call the function
        result = get_media_upload_s3_key_from_uuid(
            "12345678-1234-5678-1234-567812345678.jpg", ""
        )

        # Check results
        assert result == "bucket-folder/media/12345678-1234-5678-1234-567812345678.jpg"
        mock_get_dir.assert_called_once()
        mock_join.assert_called_once_with(
            "bucket-folder/media", "12345678-1234-5678-1234-567812345678.jpg"
        )

    def test_get_uuid_from_url_empty(self):
        "Test that the function should return empty uuid if url is empty"
        image_url = ""
        is_s3 = True
        result = get_uuid_from_url(image_url, is_s3)
        assert result == ""
    
    def test_get_uuid_from_url_local(self):
        "Test that the function works for local image_url (localhost)"
        image_url = "http://localhost:3000/" + UPLOAD_FOLDER_NAME + "/abcdefgh.png"
        is_s3 = False
        result = get_uuid_from_url(image_url, is_s3)
        assert result == "abcdefgh.png"

    def test_get_uuid_from_url_s3(self):
        "Test that the function works for s3 image_url"
        image_url = "https://dev-sensai-ind.s3.amazonaws.com/prod/media/b0e872a4-8a37-4588-834a-a366627e5cb7.mp4?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ASIAXH2X2FCR33FA63RU%2F20250409%2Fap-south-1%2Fs3%2Faws4_request&X-Amz-Date=20250409T151910Z&X-Amz-Expires=600&X-Amz-SignedHeaders=content-type%3Bhost&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEBcaCmFwLXNvdXRoLTEiRzBFAiAlRvYfrYkQCB4e0PovDFFC3%2FvYSfIGtYpz7kNTgw5PtQIhAMKI9beFAYjeZkGNeXxxYWPB0ythZ4uFuvBwIuJ3ZvwcKsAFCJD%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEQAhoMNDk3ODYzNTAxOTg3IgxckuvHS6efHPYr9CEqlAUu18ZzsTbFtcNhfcCd3QtGm0heas43AEBtyooe0kuZR5dfptT3TfiBs1OElGfzGhWarhUVpQ%2Bc%2FV6H76LvfibGskRUqSRw2Fr5r00pUG4Ljp1fK9zFEVX%2B812k3anNQEvzeOXDd9igFw6128hY%2BAl7XFowdBKFpTSukAzV1%2B4mzcMuWRc3rQi2pM%2FMYIE8E71u%2FU7RXvOXWXi1N2e%2FylZQC6nC62vKWjqLredM3ikyHXVJvvY%2FU6WeIqTAY4Z67%2BUYxXmymVTLjxNJcKyUtYLwqY4tkRGj0R3G6VELISIua0r8srlfSu9sctUHj3wuMjXw9WMEqjTQ2M8pTLRexA%2FFEXvgJu0tnGw%2FxSk0TipHELTWoOkvT9z9gK%2FJlX7T6FkWa4QXoD3xBcWCCeFbIPQ5QtCSCKfpHPMz4uGMoTYJhu%2Bqi4n7Jat8qgka8sIivjD1nq%2FQ7d%2FIys5%2BZAdCzILkUOdXwMHSTRvZNMW9Q8r7En47PuLQHpxs%2BhDBqqKNVCtY6aGaUfdmAZN0N3f%2F2C2bzLd1eVyC5Li3qLth%2FvY4eHZ6mJrLkzK5o16fwPNgJqisfFPM35UTZG%2FaTG6JXZ66Gzs2ASGsHhqw%2FkHnJ%2FFoUDTmaif4i2rHx4lwmgqRa%2BXY%2By%2F2EtT7uebzM1vgwAbJ7nJQDYrSP8fnN5M%2BPUhnRfRoAWWrDbRduHdJws2w4oeE99Hgq0WNdjg4omFNogjrZ5BSIzxx9m98Rvd1fasqPTzSISPYRC8oAb%2Bl8m9SJJldM74g%2BPvBscLbVVpDnMgQW9MoHcj0VFpVfazLAy3atm%2BfjOzhhcoPZ3bjB2QiGlvVDXflfL%2BidUbdbfFtclxv24RwYKehI8A069HIkuFHKdJxIZAw0JDavwY6sQFk%2F%2Fm9LBVeL3UGgrZj2P6ryvF1%2BMNLXouGmX%2Fpftn0j3aHAe9P1rdMfS44xhF%2F4ASGQiYlLLBn2BAYUf%2FGbDnLnU0dUVJXIbzznRVZc2DpIc3%2FUNriDRDs05ONMRPyQl55pbFVe2jLKhBvKGbyGf%2Fcs1aEyN%2Bxuvbtpx9NVxliFdwT7rpu6GnTzVL08XK8%2FKI1s%2BJa%2F56IHbX2RmOhmzAzh0BCNa7TzD9%2BwMynFRFhsnE%3D&X-Amz-Signature=91526cfdc9a7e4b8ce24d67f6db0daf1fee3b3e741b66e4ddf01ad9951c6650e"
        is_s3 = True
        result = get_uuid_from_url(image_url, is_s3)
        assert result == "b0e872a4-8a37-4588-834a-a366627e5cb7.mp4"