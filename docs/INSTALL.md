## Installation

- Install `virtualenv`
- Create a new virtual environment (choose Python3.13+)
  ```
  virtualenv -p python3.13 venv
  ```
- Activate the virtual environment
  ```
  source venv/bin/activate
  ```
- Clone the repository:
  ```
  git clone https://github.com/dalmia/sensai-backend.git
  cd sensai-backend
  ```
- Install packages
  ```
  pip install -r requirements-dev.txt
  ```
- Install `ffmpeg` and `poppler`

  For Ubuntu:
  ```
  sudo apt-get update && sudo apt-get install ffmpeg poppler-utils
  ```
  For MacOS:
  ```
  brew install ffmpeg poppler
  export PATH="/path/to/poppler/bin:$PATH"
  ```
  You can get the path to poppler using `brew list poppler`
- Copy `src/api/.env.example` to `src/api/.env` and set the OpenAI credentials. Refer to [ENV.md](./ENV.md) for more details on the environment variables. 
- Copy `src/api/.env.aws.example` to `src/api/.env.aws` and set the AWS credentials.
- Initialize the database
  ```
  cd src; python startup.py
  ```

- Running the backend api locally
    ```
    cd src; uvicorn api.main:app --reload --port 8001
    ```

    The api will be hosted on http://localhost:8001.
    The docs will be available on http://localhost:8001/docs

- Running the public api locally
    ```
    cd src; uvicorn api.main:app --port 8002
    ```
    The api will be hosted on http://localhost:8002.
    The docs will be available on http://localhost:8002/docs
  
The public API fetches data from BigQuery. To run the public API locally, you need to set up the BigQuery credentials in the `.env` file.

### Additional steps for contributors
- Set up `pre-commit` hooks. `pre-commit` should already be installed while installing requirements from the `requirements-dev.txt` file.
  ```
  pre-commit install
  ```
