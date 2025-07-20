# BigQuery Sync Documentation

This document describes the BigQuery synchronization system that automatically syncs data from the SQLite database to BigQuery for analytics and reporting.

## Overview

The BigQuery sync system provides:
- **Automatic syncing** of all database tables to BigQuery every 15 minutes
- **Change tracking** to sync only new/modified rows for efficiency
- **Schema management** with automatic BigQuery table creation
- **Error handling** with retry mechanisms and logging
- **Manual sync triggers** via API endpoints
- **Monitoring** through admin endpoints

## Architecture

### Components

1. **BigQuerySyncer** (`src/api/bq/cron.py`)
   - Main sync engine that handles data transfer
   - Manages table schemas and BigQuery operations
   - Tracks sync progress to avoid duplicate transfers

2. **Scheduler Integration** (`src/api/scheduler.py`)
   - Integrates sync jobs with the existing APScheduler
   - Runs sync every 15 minutes + daily backup at 7:30 AM IST
   - Handles error reporting via Bugsnag

3. **Admin API** (`src/api/routes/admin.py`)
   - Provides REST endpoints for sync management
   - Allows manual triggering and status monitoring

4. **Sync Tracking**
   - SQLite table `bq_sync_tracking` stores last sync info per table
   - Tracks `last_synced_row_id` to identify new/changed rows

## Configuration

### Environment Variables

Required BigQuery configuration in `.env`:

```bash
# BigQuery Configuration
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
BQ_PROJECT_NAME=your-project-id
BQ_DATASET_NAME=your-dataset-name
```

### Service Account Permissions

The Google service account needs these BigQuery permissions:
- `bigquery.datasets.get`
- `bigquery.tables.create`
- `bigquery.tables.update`
- `bigquery.tables.get`
- `bigquery.jobs.create`
- `bigquery.data.create`

## Synced Tables

The system syncs all major application tables:

### Core Tables
- `organizations` - Organization data
- `users` - User accounts and profiles
- `cohorts` - Learning cohorts
- `batches` - User batches within cohorts
- `courses` - Course definitions
- `milestones` - Course milestones/modules
- `tasks` - Learning tasks and quizzes
- `questions` - Quiz questions
- `scorecards` - Grading criteria
- `chat_history` - User-AI conversations
- `task_completions` - Task completion tracking

### Junction Tables
- `user_organizations` - User-organization relationships
- `user_cohorts` - User-cohort memberships
- `user_batches` - User-batch assignments
- `course_cohorts` - Course-cohort associations
- `course_tasks` - Course-task relationships
- `course_milestones` - Course-milestone relationships
- `question_scorecards` - Question-scorecard mappings

### System Tables
- `course_generation_jobs` - AI course generation jobs
- `task_generation_jobs` - AI task generation jobs
- `org_api_keys` - Organization API keys (hashed)
- `code_drafts` - User code draft storage

## Sync Process

### How It Works

1. **Change Detection**: For each table, the sync process:
   - Queries `bq_sync_tracking` to get the last synced row ID
   - Fetches all rows with `id > last_synced_row_id`
   - Converts datetime fields to BigQuery-compatible format

2. **BigQuery Operations**:
   - **Insert-only tables**: Uses `INSERT` for new rows (chat_history, task_completions, etc.)
   - **Updatable tables**: Uses `MERGE` operations for upserts (organizations, users, tasks, etc.)

3. **Error Handling**:
   - Creates temporary tables for MERGE operations
   - Cleans up temporary tables even on failure
   - Continues with other tables if one fails
   - Reports errors via Bugsnag if configured

4. **Progress Tracking**:
   - Updates `last_synced_row_id` after successful sync
   - Records sync timestamp for monitoring

### Sync Schedule

- **Every 15 minutes**: Incremental sync of changed data
- **Daily at 7:30 AM IST**: Full backup sync
- **On-demand**: Via admin API endpoints

## API Endpoints

### Admin Endpoints

All admin endpoints are prefixed with `/admin`:

#### Get Sync Status
```bash
GET /admin/bigquery/sync/status
```

Returns current sync job status and schedule.

#### Trigger Manual Sync
```bash
POST /admin/bigquery/sync/manual
```

Triggers an immediate full sync of all tables.

#### Sync Specific Tables
```bash
POST /admin/bigquery/sync/tables
Content-Type: application/json

["users", "organizations", "chat_history"]
```

Syncs only the specified tables.

#### Get Available Tables
```bash
GET /admin/bigquery/tables
```

Returns list of all tables configured for sync.

## Usage Examples

### Testing the Sync

Use the test script to validate configuration:

```bash
cd src
python test_bq_sync.py
```

This will:
- Check BigQuery configuration
- Test sync tracking setup
- Validate table schemas
- Run a sample sync (without writing to BigQuery)

### Manual Sync via API

```bash
# Check sync status
curl http://localhost:8001/admin/bigquery/sync/status

# Trigger manual sync
curl -X POST http://localhost:8001/admin/bigquery/sync/manual

# Sync specific tables
curl -X POST http://localhost:8001/admin/bigquery/sync/tables \
  -H "Content-Type: application/json" \
  -d '["users", "chat_history"]'
```

### Manual Sync via Code

```python
from api.bq.cron import sync_to_bigquery, sync_specific_tables

# Full sync
await sync_to_bigquery()

# Specific tables
await sync_specific_tables(["users", "organizations"])
```

## Monitoring

### Logs

Sync operations are logged with INFO level:
- Sync start/completion messages
- Row counts for each table
- Error details for troubleshooting

### Error Reporting

If Bugsnag is configured, sync errors are automatically reported with context:
- Table being synced
- Error details and stack trace
- Sync job metadata

### Health Checks

Monitor sync health by:
1. Checking admin endpoints for job status
2. Verifying BigQuery table update timestamps
3. Monitoring application logs for sync errors

## Troubleshooting

### Common Issues

1. **"BigQuery not configured" error**:
   - Check environment variables are set correctly
   - Verify service account file exists and is readable
   - Ensure service account has required permissions

2. **"Table not found" errors**:
   - BigQuery tables are created automatically on first sync
   - Check dataset exists in BigQuery console
   - Verify project/dataset names in configuration

3. **Sync appears stuck**:
   - Check scheduler status via admin endpoint
   - Look for error logs in application output
   - Verify no long-running sync jobs are blocking

4. **Data not appearing in BigQuery**:
   - Check if tables have new data in SQLite
   - Verify sync tracking table shows progress
   - Query BigQuery directly to check data freshness

### Performance Optimization

- Sync only runs when data changes (tracked by row ID)
- Large tables are processed incrementally
- Temporary tables are used for MERGE operations to avoid locks
- Batch operations are used where possible

### Recovery

If sync state becomes corrupted:

1. **Reset sync tracking** (will re-sync all data):
   ```sql
   DELETE FROM bq_sync_tracking WHERE table_name = 'table_name';
   ```

2. **Drop BigQuery tables** (forces recreation):
   ```sql
   DROP TABLE `project.dataset.table_name`;
   ```

3. **Trigger manual sync** to rebuild data.

## Schema Management

### Adding New Tables

To add a new table to sync:

1. Add table name to imports in `src/api/bq/cron.py`
2. Add schema definition method (e.g., `_get_new_table_schema()`)
3. Add to `tables_to_sync` dictionary in `BigQuerySyncer.__init__()`
4. Determine if table supports updates (add to `_table_supports_updates()` if needed)

### Schema Changes

When SQLite schema changes:
1. Update the corresponding BigQuery schema method
2. BigQuery will automatically add new columns on next sync
3. For column type changes, manual BigQuery schema migration may be needed

## Security Considerations

- Service account credentials are stored securely
- API keys are hashed before syncing to BigQuery
- Admin endpoints should be protected in production
- BigQuery dataset access should be restricted appropriately

## Performance Metrics

Typical sync performance:
- Small tables (< 1K rows): < 5 seconds
- Medium tables (1K-10K rows): 10-30 seconds  
- Large tables (> 10K rows): 1-5 minutes
- Full initial sync: 10-30 minutes depending on data size

The sync process is designed to be efficient and non-blocking to application performance. 