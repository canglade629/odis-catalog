# API Key Authentication System

This document describes how to use the multi-user API key authentication system.

## Overview

The API key system allows you to:
- Generate secure API keys for different users (each with their own `user_id`, e.g. "salma", "ronan")
- Create **read-only** users by default, or **admin** users by setting `is_admin: true`
- Validate API keys on each request; the app view (admin vs user) depends on the `is_admin` flag
- Revoke or delete API keys
- Track usage (last_used_at timestamp)

## Security Features

- **Secure Generation**: Keys use 256 bits of entropy via Python's `secrets` module
- **Hashed Storage**: Keys are hashed with SHA-256 before storage in Firestore
- **Bearer Token Auth**: Industry-standard `Authorization: Bearer` header
- **Soft Delete**: Keys can be revoked (active=false) for audit trail
- **Admin Protection**: Key management endpoints protected by separate admin secret

## Configuration

### Environment Variables

Update your `.env` file:

```bash
ADMIN_SECRET=your-secure-admin-secret-here
```

The `ADMIN_SECRET` protects the admin endpoints for creating/managing API keys. Also configure the GCP project, BigQuery datasets and Firestore database (see the [README](../README.md#local-run)).

## Managing API Keys (Admin API)

All admin endpoints require the `ADMIN_SECRET` in the Authorization header.

### Create a new API key

By default, new keys are **read-only** (certified silver tables, no pipelines/files). Set `is_admin: true` to create an admin key (full access: pipelines, catalogue, certifications, key management).

```bash
POST /admin/api-keys
Authorization: Bearer your-admin-secret-here
Content-Type: application/json

{
  "user_id": "salma",
  "is_admin": true
}
```

Response:
```json
{
  "api_key": "sk_live_...",
  "user_id": "salma",
  "is_admin": true,
  "created_at": "2025-01-10T10:30:00.000000",
  "message": "API key created successfully. Save this key - it will not be shown again."
}
```

### Current user (for UI / view mode)

Authenticated clients can call `GET /api/me` with their API key to get `{ "user_id": "...", "is_admin": true|false }`. The frontend uses this to show admin vs user view.

### List all API keys

```bash
GET /admin/api-keys
Authorization: Bearer your-admin-secret-here
```

Response entries include `user_id`, `is_admin`, `created_at`, `last_used_at`, `active`.

### Revoke an API key (soft delete)

```bash
DELETE /api/admin/api-keys/revoke
Authorization: Bearer your-admin-secret-here
Content-Type: application/json

{
  "api_key": "sk_live_..."
}
```

### Permanently delete an API key

```bash
DELETE /api/admin/api-keys/delete
Authorization: Bearer your-admin-secret-here
Content-Type: application/json

{
  "api_key": "sk_live_..."
}
```

## Using API Keys

### For API Consumers

Users must include their API key in the `Authorization` header with the `Bearer` scheme:

```bash
GET /api/endpoint
Authorization: Bearer sk_live_YOUR_API_KEY
```

Example with curl:

```bash
curl -H "Authorization: Bearer sk_live_YOUR_API_KEY" \
  https://odace.services.d4g.fr/api/endpoint
```

Example with Python requests:

```python
import requests

headers = {
    "Authorization": "Bearer sk_live_YOUR_API_KEY"
}
response = requests.get("https://odace.services.d4g.fr/api/endpoint", headers=headers)
```

### Authentication Errors

- **401 Unauthorized**: API key is missing from the request
- **403 Forbidden**: API key is invalid or has been revoked

## Firestore Storage

API keys are stored in the `api_keys` collection, one document per key:

- Document id: SHA-256 hash of the API key
- `user_id`: User email or identifier
- `is_admin`: Admin access flag
- `created_at`, `last_used_at`: Timestamps (`last_used_at` is refreshed at most every 5 minutes)
- `active`: Whether the key is active (false = revoked)

See the Firestore data model in the [README](../README.md#firestore-data-model).

## Migration from Old System

The old single `API_KEY` system has been replaced. You need to:

1. **Update your `.env` file**: Add `ADMIN_SECRET` and the GCP vars (`GCP_PROJECT_ID`, `BIGQUERY_DATASET_PREFIX`, `FIRESTORE_DATABASE`).
2. **Create API keys** via the Admin API (see above).
3. **Update client code** to use `Authorization: Bearer` header.
4. **Distribute API keys** to your users securely.

## Best Practices

1. **Keep API keys secret**: Never commit them to version control
2. **Use HTTPS**: Always use HTTPS in production to protect keys in transit
3. **Rotate keys regularly**: Delete old keys and generate new ones periodically
4. **Monitor usage**: Check `last_used_at` via the list endpoint to identify unused keys
5. **Revoke before delete**: Use revocation first to ensure no active usage before permanent deletion
6. **Secure admin secret**: Protect your `ADMIN_SECRET` as it controls all key management

## Troubleshooting

### "API key is missing"
- Ensure you're sending the `Authorization: Bearer <key>` header
- Check for typos in the header name

### "Invalid or inactive API key"
- Verify the key hasn't been revoked (list keys via Admin API)
- Ensure the key exists in the `api_keys` Firestore collection

### Firestore connection issues
- Verify `GCP_PROJECT_ID` and `FIRESTORE_DATABASE` are set correctly
- Ensure the credentials have Firestore access (`roles/datastore.user`): locally `gcloud auth application-default login`, on Cloud Run the service's runtime service account. See the [README](../README.md#deployment).

## Support

For issues or questions, please refer to the main README.md or contact your system administrator.
