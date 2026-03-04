# API Access Control - Updated

## Overview

The API now has **two levels of access**:

1. **Regular User API Keys** - Limited to read-only operations
2. **Admin Secret** - Full access to all operations

## Regular User API Keys

### What They CAN Do:
✅ **Query Metadata**:
- `GET /api/pipeline/list` - List available pipelines
- `GET /api/pipeline/status/{run_id}` - Get pipeline status
- `GET /api/pipeline/history` - View pipeline execution history
- `GET /api/data/catalog` - Get data catalog
- `GET /api/data/catalog/silver` - Get silver table catalog with certification status
- `GET /api/data/catalog/silver/{table_name}` - Get table details
- `GET /api/data/table/{layer}/{table}` - Get table metadata
- `GET /api/files/list` - List files
- `GET /api/jobs` - List jobs
- `GET /api/jobs/{job_id}` - Get job details
- `GET /api/jobs/logs/stream` - View logs

✅ **Query Certified Silver Tables**:
- `POST /api/data/preview/silver/{table_name}` - Preview certified silver tables only
- `POST /api/data/query` - SQL queries on certified silver tables only

### What They CANNOT Do:
❌ **Execute Pipelines**:
- `POST /api/bronze/{pipeline_name}` - ❌ Requires admin
- `POST /api/silver/{pipeline_name}` - ❌ Requires admin
- `POST /api/gold/{pipeline_name}` - ❌ Requires admin
- `POST /api/pipeline/run` - ❌ Requires admin

❌ **Manage Files**:
- `POST /api/files/upload` - ❌ Requires admin

❌ **Manage Jobs**:
- `POST /api/jobs/{job_id}/cancel` - ❌ Requires admin

❌ **Query Uncertified Tables**:
- Cannot preview or query uncertified silver tables
- Cannot access bronze or gold tables

## Admin Secret

The admin secret (`ADMIN_SECRET` environment variable) provides full access to:

✅ Everything regular users can do, PLUS:
- Execute any pipeline (bronze, silver, gold)
- Upload files to raw data storage
- Cancel running jobs
- Certify/uncertify tables
- Query ANY table (certified or not, any layer)
- Create/manage API keys

## API Key vs Admin Secret

| Feature | Regular API Key | Admin Secret |
|---------|----------------|--------------|
| View pipelines & metadata | ✅ | ✅ |
| View jobs & logs | ✅ | ✅ |
| Query certified silver tables | ✅ | ✅ |
| Query uncertified tables | ❌ | ✅ |
| Query bronze/gold tables | ❌ | ✅ |
| Execute pipelines | ❌ | ✅ |
| Upload files | ❌ | ✅ |
| Cancel jobs | ❌ | ✅ |
| Certify tables | ❌ | ✅ |
| Manage API keys | ❌ | ✅ |

## Usage Examples

### Regular User - Query Certified Table
```bash
# Works if table is certified
curl -X POST https://your-deployment-url/api/data/query \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM silver_dim_commune LIMIT 10"}'
```

### Regular User - Try to Run Pipeline
```bash
# Returns: {"detail": "Invalid admin secret"}
curl -X POST https://your-deployment-url/api/pipeline/run \
  -H "Authorization: Bearer YOUR_API_KEY"
```

### Admin - Run Pipeline
```bash
# Works with admin secret
curl -X POST https://your-deployment-url/api/pipeline/run \
  -H "Authorization: Bearer YOUR_ADMIN_SECRET"
```

### Admin - Certify Table
```bash
curl -X POST https://your-deployment-url/admin/tables/certify \
  -H "Authorization: Bearer YOUR_ADMIN_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"table_name": "dim_commune", "layer": "silver"}'
```

## Error Messages

### Invalid API Key
```json
{
  "detail": "Invalid or inactive API key"
}
```

### Missing Authorization
```json
{
  "detail": "API key is missing. Please provide Authorization: Bearer header."
}
```

### Admin Required
```json
{
  "detail": "Invalid admin secret"
}
```

### Uncertified Table (Regular User)
```json
{
  "detail": "Table dim_commune is not certified for public use. Please contact an administrator."
}
```

## Security Model

1. **API Keys** are stored in PostgreSQL with hashed values
2. **Admin Secret** is set via environment (e.g. Coolify env vars)
3. **Table Certifications** are stored in PostgreSQL
4. All authenticated endpoints use rate limiting (60 requests/minute)
5. Regular users are isolated from:
   - Pipeline execution (prevent resource exhaustion)
   - File uploads (prevent data corruption)
   - Uncertified data access (data quality control)

## Creating User API Keys

Admins can create API keys for users:

```bash
curl -X POST https://your-deployment-url/admin/api-keys \
  -H "Authorization: Bearer YOUR_ADMIN_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user@example.com"}'
```

Response:
```json
{
  "api_key": "generated_api_key_here",
  "user_id": "user@example.com",
  "created_at": "2025-12-05T15:00:00.000Z",
  "message": "API key created successfully. Save this key - it will not be shown again."
}
```

**Important**: The API key is shown only once during creation!

## Summary

The API now properly restricts regular users to **read-only operations** and **certified silver table queries only**. Pipeline execution, file management, and table certification require the **admin secret**.


