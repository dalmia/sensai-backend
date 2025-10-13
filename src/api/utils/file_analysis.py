import os
import zipfile
import tempfile
from typing import Dict, List, Tuple
from pathlib import Path


def extract_zip_file(zip_file_path: str) -> Tuple[str, List[str]]:
    """
    Extract a ZIP file and return the extraction directory and list of files.
    
    Args:
        zip_file_path: Path to the ZIP file
        
    Returns:
        Tuple of (extraction_directory, list_of_file_paths)
    """
    # Create a temporary directory for extraction
    temp_dir = tempfile.mkdtemp()
    
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
            
        # Get list of all extracted files
        extracted_files = []
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                file_path = os.path.join(root, file)
                extracted_files.append(file_path)
                
        return temp_dir, extracted_files
        
    except zipfile.BadZipFile:
        raise ValueError("Invalid ZIP file format")
    except Exception as e:
        raise ValueError(f"Error extracting ZIP file: {str(e)}")



def cleanup_temp_directory(temp_dir: str):
    """
    Clean up temporary directory and its contents.
    
    Args:
        temp_dir: Path to temporary directory
    """
    try:
        import shutil
        shutil.rmtree(temp_dir)
    except Exception as e:
        # Log error but don't raise
        print(f"Warning: Could not clean up temporary directory {temp_dir}: {e}")


def extract_submission_file(file_uuid: str) -> Dict[str, any]:
    """
    Extract a submission ZIP file and return the raw extracted data.
    
    Args:
        file_uuid: UUID of the uploaded file
        
    Returns:
        Dictionary containing extracted file data
    """
    from api.settings import settings
    from api.utils.s3 import download_file_from_s3_as_bytes, get_media_upload_s3_key_from_uuid
    
    # Download the file
    if settings.s3_folder_name:
        try:
            file_data = download_file_from_s3_as_bytes(
                get_media_upload_s3_key_from_uuid(file_uuid, "zip")
            )
        except:
            # Fallback to local file
            file_path = os.path.join(settings.local_upload_folder, f"{file_uuid}.zip")
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_uuid}")
            with open(file_path, 'rb') as f:
                file_data = f.read()
    else:
        file_path = os.path.join(settings.local_upload_folder, f"{file_uuid}.zip")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_uuid}")
        with open(file_path, 'rb') as f:
            file_data = f.read()
    
    # Create temporary ZIP file
    temp_zip_path = None
    temp_extract_dir = None
    
    try:
        # Write to temporary ZIP file
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_zip:
            temp_zip.write(file_data)
            temp_zip_path = temp_zip.name
        
        # Extract ZIP file
        temp_extract_dir, extracted_files = extract_zip_file(temp_zip_path)
        
        # Read all file contents
        file_contents = {}
        for file_path in extracted_files:
            try:
                file_name = os.path.basename(file_path)
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    file_contents[file_name] = content
            except UnicodeDecodeError:
                # Skip files that can't be decoded as text
                continue
            except Exception:
                # Skip files that can't be read
                continue
        
        # Prepare extraction result
        result = {
            "file_uuid": file_uuid,
            "extracted_files_count": len(extracted_files),
            "file_contents": file_contents,
            "extracted_files": extracted_files
        }
        
        return result
        
    finally:
        # Clean up temporary files
        if temp_zip_path and os.path.exists(temp_zip_path):
            os.unlink(temp_zip_path)
        if temp_extract_dir:
            cleanup_temp_directory(temp_extract_dir)
