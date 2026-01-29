FROM python:3.13.7-slim-bookworm

# Install uv package manager
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    curl \
    wget \
    fontconfig \
    libfreetype6 \
    libjpeg62-turbo \
    libpng16-16 \
    libx11-6 \
    libxcb1 \
    libxext6 \
    libxrender1 \
    xfonts-75dpi \
    xfonts-base \
    ffmpeg \
    poppler-utils

# Install Node.js and npm
RUN curl -fsSL https://deb.nodesource.com/setup_lts.x | bash -
RUN apt-get install -y nodejs git

# Install libssl1.1
RUN wget http://archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.1f-1ubuntu2_amd64.deb
RUN dpkg -i libssl1.1_1.1.1f-1ubuntu2_amd64.deb

# Install wkhtmltopdf
RUN wget https://github.com/wkhtmltopdf/packaging/releases/download/0.12.6.1-2/wkhtmltox_0.12.6.1-2.bullseye_amd64.deb \
    && dpkg -i wkhtmltox_0.12.6.1-2.bullseye_amd64.deb \
    && apt-get install -f \
    && rm wkhtmltox_0.12.6.1-2.bullseye_amd64.deb

# Verify wkhtmltopdf installation
RUN wkhtmltopdf --version

# Set working directory
WORKDIR /app

# Copy dependency files to the container
COPY pyproject.toml uv.lock ./

# Install app dependencies using uv (faster than pip)
RUN uv sync --frozen --no-dev

# Copy source code
COPY src ./src

# Remove any local .env files that may have been copied
RUN test -f ./src/api/.env && rm -f ./src/api/.env || true
RUN test -f ./src/api/.env.aws && rm -f ./src/api/.env.aws || true

# Clean up
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Expose the port on which your FastAPI app listens
EXPOSE 8001

# Set Python path so 'api' module can be found
ENV PYTHONPATH=/app/src

# Run the application (uv run must be from /app where .venv is)
CMD ["bash", "-c", "uv run python /app/src/startup.py && uv run uvicorn api.main:app --host 0.0.0.0 --port 8001"]
