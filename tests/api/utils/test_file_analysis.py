import pytest
import os
import tempfile
import zipfile
import shutil
from unittest.mock import patch, mock_open, MagicMock
from pathlib import Path
from src.api.utils.file_analysis import (
    extract_zip_file,
    extract_submission_file,
)

class TestFileAnalysis:
    """Test file analysis utility functions."""

    def test_extract_zip_file_success(self):
        """Test successful ZIP file extraction."""
        # Create a temporary ZIP file with test content
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_zip:
            with zipfile.ZipFile(temp_zip.name, 'w') as zip_ref:
                zip_ref.writestr('test1.txt', 'Hello World 1')
                zip_ref.writestr('test2.txt', 'Hello World 2')
                zip_ref.writestr('subdir/test3.txt', 'Hello World 3')
        
        try:
            # Extract the ZIP file
            extract_dir, extracted_files = extract_zip_file(temp_zip.name)
            
            # Verify extraction directory exists
            assert os.path.exists(extract_dir)
            assert os.path.isdir(extract_dir)
            
            # Verify all files were extracted
            assert len(extracted_files) == 3
            
            # Verify file contents
            for file_path in extracted_files:
                assert os.path.exists(file_path)
                assert os.path.isfile(file_path)
                
                # Read and verify content
                with open(file_path, 'r') as f:
                    content = f.read()
                    assert 'Hello World' in content
            
            # Clean up extraction directory
            shutil.rmtree(extract_dir)
            
        finally:
            # Clean up temporary ZIP file
            if os.path.exists(temp_zip.name):
                os.unlink(temp_zip.name)

    def test_extract_zip_file_invalid_zip(self):
        """Test extraction with invalid ZIP file."""
        # Create a temporary file that's not a valid ZIP
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
            temp_file.write(b'This is not a ZIP file')
            temp_file.flush()
        
        try:
            # Should raise ValueError for invalid ZIP
            with pytest.raises(ValueError, match="Invalid ZIP file format"):
                extract_zip_file(temp_file.name)
                
        finally:
            if os.path.exists(temp_file.name):
                os.unlink(temp_file.name)

    def test_extract_zip_file_nonexistent_file(self):
        """Test extraction with nonexistent file."""
        with pytest.raises(ValueError, match="Error extracting ZIP file"):
            extract_zip_file("/nonexistent/file.zip")


    @patch("src.api.utils.file_analysis.shutil.rmtree")
    @patch("api.utils.s3.download_file_from_s3_as_bytes")
    @patch("api.utils.s3.get_media_upload_s3_key_from_uuid")
    @patch("api.settings.settings")
    def test_extract_submission_file_s3_success(
        self, mock_settings, mock_get_s3_key, mock_download_s3, mock_rmtree
    ):
        """Test successful submission file extraction from S3."""
        # Setup mocks
        mock_settings.s3_folder_name = "test-bucket"
        mock_get_s3_key.return_value = "uploads/test-uuid.zip"
        mock_download_s3.return_value = self._create_test_zip_bytes()
        
        # Mock the extract_zip_file function to avoid actual file system operations
        with patch("src.api.utils.file_analysis.extract_zip_file") as mock_extract:
            mock_extract.return_value = (
                "/tmp/extract_dir",
                ["/tmp/extract_dir/test1.txt", "/tmp/extract_dir/test2.txt"]
            )
            
            # Mock file reading for extracted files
            with patch("builtins.open", mock_open(read_data="Hello World")):
                result = extract_submission_file("test-uuid")
        
        # Verify result
        assert result["file_uuid"] == "test-uuid"
        assert result["extracted_files_count"] == 2
        assert "file_contents" in result
        
        # Verify S3 calls
        mock_get_s3_key.assert_called_once_with("test-uuid", "zip")
        mock_download_s3.assert_called_once_with("uploads/test-uuid.zip")


    @patch("src.api.utils.file_analysis.shutil.rmtree")
    @patch("api.settings.settings")
    def test_extract_submission_file_local_success(self, mock_settings, mock_rmtree):
        """Test successful submission file extraction from local storage."""
        # Setup mocks
        mock_settings.s3_folder_name = None
        mock_settings.local_upload_folder = "/tmp/uploads"
        
        # Create test ZIP file
        test_zip_path = "/tmp/uploads/test-uuid.zip"
        test_zip_bytes = self._create_test_zip_bytes()
        
        # Mock file operations
        with patch("os.path.exists", return_value=True), \
             patch("src.api.utils.file_analysis.extract_zip_file") as mock_extract:
            
            mock_extract.return_value = (
                "/tmp/extract_dir",
                ["/tmp/extract_dir/test1.txt", "/tmp/extract_dir/test2.txt"]
            )
            
            # Mock file reading - need to mock both the ZIP file read and the extracted file reads
            def mock_file_open(file_path, mode, **kwargs):
                if mode == 'rb' and 'test-uuid.zip' in file_path:
                    return mock_open(read_data=test_zip_bytes)()
                elif mode == 'r':
                    return mock_open(read_data="Hello World")()
                else:
                    return mock_open(read_data=b"")()
            
            with patch("builtins.open", side_effect=mock_file_open):
                result = extract_submission_file("test-uuid")
        
        # Verify result
        assert result["file_uuid"] == "test-uuid"
        assert result["extracted_files_count"] == 2
        assert "file_contents" in result

    @patch("api.settings.settings")
    def test_extract_submission_file_local_not_found(self, mock_settings):
        """Test submission file extraction when local file not found."""
        # Setup mocks
        mock_settings.s3_folder_name = None
        mock_settings.local_upload_folder = "/tmp/uploads"
        
        # Mock file not found
        with patch("os.path.exists", return_value=False):
            with pytest.raises(FileNotFoundError, match="File not found: test-uuid"):
                extract_submission_file("test-uuid")


    @patch("src.api.utils.file_analysis.shutil.rmtree")
    @patch("src.api.utils.file_analysis.extract_zip_file")
    @patch("api.settings.settings")
    def test_extract_submission_file_unicode_decode_error(
        self, mock_settings, mock_extract, mock_rmtree
    ):
        """Test handling of Unicode decode errors when reading files."""
        # Setup mocks
        mock_settings.s3_folder_name = None
        mock_settings.local_upload_folder = "/tmp/uploads"
        mock_extract.return_value = (
            "/tmp/extract_dir",
            ["/tmp/extract_dir/binary_file.bin", "/tmp/extract_dir/text_file.txt"]
        )
        
        # Mock file operations
        with patch("os.path.exists", return_value=True):
            
            # Mock file reading with UnicodeDecodeError for binary file
            def mock_file_open(file_path, mode, **kwargs):
                if mode == 'rb' and 'test-uuid.zip' in file_path:
                    return mock_open(read_data=b"zip content")()
                elif "binary_file.bin" in file_path:
                    raise UnicodeDecodeError("utf-8", b"", 0, 1, "invalid start byte")
                else:
                    return mock_open(read_data="text content")()
            
            with patch("builtins.open", side_effect=mock_file_open):
                result = extract_submission_file("test-uuid")
        
        # Verify result - should only contain the text file (binary files are filtered out)
        assert result["file_uuid"] == "test-uuid"
        assert result["extracted_files_count"] == 1  # Only .txt file is allowed
        assert len(result["file_contents"]) == 1  # Only text file should be included
        assert "text_file.txt" in result["file_contents"]

    def _create_test_zip_bytes(self):
        """Helper method to create test ZIP file bytes."""
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_zip:
            with zipfile.ZipFile(temp_zip.name, 'w') as zip_ref:
                zip_ref.writestr('test1.txt', 'Hello World 1')
                zip_ref.writestr('test2.txt', 'Hello World 2')
            
            with open(temp_zip.name, 'rb') as f:
                zip_bytes = f.read()
            
            # Clean up
            os.unlink(temp_zip.name)
            return zip_bytes

    @patch("src.api.utils.file_analysis.shutil.rmtree")
    @patch("src.api.utils.file_analysis.extract_zip_file")
    @patch("api.settings.settings")
    def test_extract_submission_file_general_exception_handling(
        self, mock_settings, mock_extract, mock_rmtree
    ):
        """Test handling of general exceptions when reading files - should skip files that can't be read."""
        # Setup mocks
        mock_settings.s3_folder_name = None
        mock_settings.local_upload_folder = "/tmp/uploads"
        mock_extract.return_value = (
            "/tmp/extract_dir",
            ["/tmp/extract_dir/test1.txt", "/tmp/extract_dir/test2.txt"]
        )
        
        # Mock file operations
        with patch("os.path.exists", return_value=True):
            
            # Mock file reading with general exception
            def mock_file_open(file_path, mode, **kwargs):
                if mode == 'rb' and 'test-uuid.zip' in file_path:
                    return mock_open(read_data=b"zip content")()
                elif "test1.txt" in file_path:
                    raise PermissionError("Permission denied")
                else:
                    return mock_open(read_data="text content")()
            
            with patch("builtins.open", side_effect=mock_file_open):
                result = extract_submission_file("test-uuid")
        
        # Verify result - should only contain the file that could be read
        assert result["file_uuid"] == "test-uuid"
        assert result["extracted_files_count"] == 2  # Both files were found
        assert len(result["file_contents"]) == 1  # Only one file should be included (test2.txt)
        assert "test2.txt" in result["file_contents"]
        assert "test1.txt" not in result["file_contents"]
