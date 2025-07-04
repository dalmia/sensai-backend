FROM python:3.13.1-slim-bookworm

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

# Copy requirements.txt to the container
COPY requirements.txt ./

# Install app dependencies
RUN pip install -r requirements.txt

COPY src /src

RUN test -f /src/api/.env && rm -f /src/api/.env || true
RUN test -f /src/api/.env.aws && rm -f /src/api/.env.aws || true

# Expose the port on which your FastAPI app listens
# EXPOSE 8001
EXPOSE 8002

# Only expose one port where everything is hosted
EXPOSE 8501

ARG S3_BUCKET_NAME

ARG S3_FOLDER_NAME

ARG ENV

ARG OPENAI_API_KEY

ARG GOOGLE_CLIENT_ID

ARG BUGSNAG_API_KEY

ARG SLACK_USER_SIGNUP_WEBHOOK_URL

ARG SLACK_COURSE_CREATED_WEBHOOK_URL

ARG SLACK_USAGE_STATS_WEBHOOK_URL

ARG PHOENIX_ENDPOINT

ARG PHOENIX_API_KEY

RUN printf "OPENAI_API_KEY=$OPENAI_API_KEY\nGOOGLE_CLIENT_ID=$GOOGLE_CLIENT_ID\nS3_BUCKET_NAME=$S3_BUCKET_NAME\nS3_FOLDER_NAME=$S3_FOLDER_NAME\nENV=$ENV\nBUGSNAG_API_KEY=$BUGSNAG_API_KEY\nSLACK_USER_SIGNUP_WEBHOOK_URL=$SLACK_USER_SIGNUP_WEBHOOK_URL\nSLACK_COURSE_CREATED_WEBHOOK_URL=$SLACK_COURSE_CREATED_WEBHOOK_URL\nSLACK_USAGE_STATS_WEBHOOK_URL=$SLACK_USAGE_STATS_WEBHOOK_URL\nPHOENIX_ENDPOINT=$PHOENIX_ENDPOINT\nPHOENIX_API_KEY=$PHOENIX_API_KEY" >> /src/api/.env

# Clean up
RUN apt-get clean && rm -rf /var/lib/apt/lists/*
