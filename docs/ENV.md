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

### ENV (optional)
The environment the app is running in (staging/production).

### SLACK_USER_SIGNUP_WEBHOOK_URL (optional)
The Slack webhook URL for sending notifications to the user signup channel.

### SLACK_COURSE_CREATED_WEBHOOK_URL (optional)
The Slack webhook URL for sending notifications about new courses created.

### SLACK_USAGE_STATS_WEBHOOK_URL (optional)
The Slack webhook URL for sending platform usage statistics notifications.

### PHOENIX_ENDPOINT (optional)
The endpoint for the self-hosted Phoenix instance. This is only used for local development.

### PHOENIX_API_KEY (optional)
The API key for accessing the Phoenix API for a secure self-hosted instance.