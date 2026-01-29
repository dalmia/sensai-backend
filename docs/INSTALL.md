## Installation

[uv](https://docs.astral.sh/uv/) is a fast Python package manager that we use for this project.

- Install uv:

  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```

- Clone the repository:

  ```bash
  git clone https://github.com/dalmia/sensai-backend.git
  cd sensai-backend
  ```

- Install dependencies (including dev dependencies):

  ```bash
  uv sync --all-extras
  ```

- Install `ffmpeg` and `poppler`

  For Ubuntu:

  ```bash
  sudo apt-get update && sudo apt-get install ffmpeg poppler-utils
  ```

  For MacOS:

  ```bash
  brew install ffmpeg poppler
  export PATH="/path/to/poppler/bin:$PATH"
  ```

  You can get the path to poppler using `brew list poppler`

- Copy `src/api/.env.example` to `src/api/.env` and set the OpenAI credentials. Refer to [ENV.md](./ENV.md) for more details on the environment variables.
- Copy `src/api/.env.aws.example` to `src/api/.env.aws` and set the AWS credentials.
- Initialize the database and run the backend API locally:

  ```bash
  cd src
  uv run python startup.py && uv run uvicorn api.main:app --reload --port 8001
  ```

  The API will be hosted on http://localhost:8001.
  The docs will be available on http://localhost:8001/docs

- Running the public API locally:
  ```bash
  cd src
  uv run uvicorn api.main:app --port 8002
  ```
  The API will be hosted on http://localhost:8002.
  The docs will be available on http://localhost:8002/docs

The public API fetches data from BigQuery. To run the public API locally, you need to set up the BigQuery credentials in the `.env` file.

### Additional steps for contributors

- Set up `pre-commit` hooks. `pre-commit` should already be installed while installing dev dependencies.
  ```bash
  pre-commit install
  ```
