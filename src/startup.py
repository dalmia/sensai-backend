from api.db import init_db
import os
import asyncio
from api.config import UPLOAD_FOLDER_NAME

root_dir = os.path.dirname(os.path.abspath(__file__))

if __name__ == "__main__":
    asyncio.run(init_db())

    # create uploads folder
    if not os.path.exists("/appdata"):
        upload_folder = os.path.join(root_dir, UPLOAD_FOLDER_NAME)
        if not os.path.exists(upload_folder):
            os.makedirs(upload_folder)
