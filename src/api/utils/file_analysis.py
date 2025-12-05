import os
import zipfile
import tempfile
import shutil
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


def extract_submission_file(file_uuid: str) -> Dict[str, any]:
    """
    Extract a submission ZIP file and return the raw extracted data.
    Only extracts files with allowed code extensions.
    
    Args:
        file_uuid: UUID of the uploaded file
        
    Returns:
        Dictionary containing extracted file data
    """
    from api.settings import settings
    from api.utils.s3 import download_file_from_s3_as_bytes, get_media_upload_s3_key_from_uuid
    
    # Define allowed file extensions for code submissions
    ALLOWED_EXTENSIONS = {
        '.js', '.jsx', '.ts', '.tsx',   # JavaScript/TypeScript
        '.html', '.htm',                # HTML
        '.css', '.scss', '.sass',       # CSS
        '.py',                          # Python
        '.java',                        # Java
        '.c', '.cpp', '.h', '.hpp',     # C/C++
        '.go', '.rs', '.php', '.rb',    # Other languages
        '.json', '.yaml', '.yml',       # Config files
        '.txt', '.md',                  # Text files
        '.sh', '.bat', '.ps1',          # Shell scripts
    }
    
    # Download the file
    if settings.s3_folder_name:
        file_data = download_file_from_s3_as_bytes(
            get_media_upload_s3_key_from_uuid(file_uuid, "zip")
        )
    else:
        file_path = os.path.join(settings.local_upload_folder, f"{file_uuid}.zip")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_uuid}")
        with open(file_path, 'rb') as f:
            file_data = f.read()
    
    # Create temporary ZIP file and handle extraction within its context
    temp_extract_dir = None
    with tempfile.NamedTemporaryFile(suffix='.zip') as temp_zip:
        temp_zip.write(file_data)
        temp_zip.flush()

        # Extract ZIP file
        temp_extract_dir, extracted_files = extract_zip_file(temp_zip.name)

        # Filter files to include only relevant code files
        code_files = []
        file_contents = {}

        for file_path in extracted_files:
            # Get file extension
            file_ext = os.path.splitext(file_path)[1].lower()

            # Only include files with allowed extensions
            if file_ext in ALLOWED_EXTENSIONS:
                code_files.append(file_path)

        # Read content of filtered code files
        for file_path in code_files:
            try:
                relative_path = os.path.relpath(file_path, temp_extract_dir).replace(os.sep, '/')
                with open(file_path, 'r', encoding='utf-8') as f:
                    file_contents[relative_path] = f.read()
            except Exception:
                continue

    # Clean up extracted directory after temp file is removed
    if temp_extract_dir:
        shutil.rmtree(temp_extract_dir)

    # Prepare extraction result
    return {
        "file_uuid": file_uuid,
        "extracted_files_count": len(code_files),
        "file_contents": file_contents,
    }