# Environment Variables

### OPENAI_API_KEY

The API key for the OpenAI API.

### GOOGLE_CLIENT_ID

The client ID for the Google OAuth2.0 client.

## Deployment-Only Variables

### S3_BUCKET_NAME

The name of the S3 bucket used for storing files uploaded by users.

### S3_FOLDER_NAME

The name of the S3 folder within the S3 bucket. We use the same bucket for dev and prod but with different folder names.

### BUGSNAG_API_KEY (optional)

The API key for the Bugsnag (used for error tracking).

### SENTRY_DSN (optional)

The DSN for Sentry (used for error tracking and performance monitoring).

### SENTRY_ENVIRONMENT (optional)

The environment the app is running in (development/staging/production).

### ENV (optional)

The environment the app is running in (staging/production).

### SLACK_USER_SIGNUP_WEBHOOK_URL (optional)

The Slack webhook URL for sending notifications to the user signup channel.

### SLACK_ALERT_WEBHOOK_URL (optional)

The Slack webhook URL for sending alerts.

### SLACK_COURSE_CREATED_WEBHOOK_URL (optional)

The Slack webhook URL for sending notifications about new courses created.

### SLACK_USAGE_STATS_WEBHOOK_URL (optional)

The Slack webhook URL for sending platform usage statistics notifications.

### LANGFUSE_SECRET_KEY (optional)

The secret key for the Langfuse instance.

### LANGFUSE_PUBLIC_KEY (optional)

The public key for the Langfuse instance.

### LANGFUSE_TRACING_ENVIRONMENT (optional)

The tracing environment for the Langfuse instance (development, staging, production).

### LANGFUSE_HOST (optional)

The endpoint for the self-hosted Langfuse instance. This is only used for local development.

### GOOGLE_APPLICATION_CREDENTIALS (optional)

The absolute path to the GCP service account credentials file.

### BQ_PROJECT_NAME (optional)

The name of the BigQuery project to use for storing data.

### BQ_DATASET_NAME (optional)

The name of the BigQuery dataset to use for storing data.
