# Table Certification API Reference

## Admin Endpoints

All admin endpoints require authentication via `ADMIN_SECRET` in the Authorization header:
```
Authorization: Bearer YOUR_ADMIN_SECRET
```

### Certify a Table

**Endpoint:** `POST /admin/tables/certify`

**Description:** Certify a table for public use by API users.

**Request Body:**
```json
{
  "table_name": "dim_commune",
  "layer": "silver"
}
```

**Response (200 OK):**
```json
{
  "layer": "silver",
  "table_name": "dim_commune",
  "certified": true,
  "certified_at": "2025-12-05T15:30:00.000Z",
  "certified_by": "admin"
}
```

**Example:**
```bash
curl -X POST http://localhost:8000/admin/tables/certify \
  -H "Authorization: Bearer YOUR_ADMIN_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"table_name": "dim_commune", "layer": "silver"}'
```

---

### Uncertify a Table

**Endpoint:** `POST /admin/tables/uncertify`

**Description:** Remove certification from a table.

**Request Body:**
```json
{
  "table_name": "dim_commune",
  "layer": "silver"
}
```

**Response (200 OK):**
```json
{
  "message": "Table silver.dim_commune uncertified successfully"
}
```

**Example:**
```bash
curl -X POST http://localhost:8000/admin/tables/uncertify \
  -H "Authorization: Bearer YOUR_ADMIN_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"table_name": "dim_commune", "layer": "silver"}'
```

---

### List All Certifications

**Endpoint:** `GET /admin/tables/certifications`

**Description:** Get a list of all table certifications.

**Response (200 OK):**
```json
{
  "certifications": [
    {
      "layer": "silver",
      "table_name": "dim_commune",
      "certified": true,
      "certified_at": "2025-12-05T15:30:00.000Z",
      "certified_by": "admin"
    },
    {
      "layer": "silver",
      "table_name": "fact_loyer_annonce",
      "certified": true,
      "certified_at": "2025-12-05T16:00:00.000Z",
      "certified_by": "admin"
    }
  ]
}
```

**Example:**
```bash
curl -X GET http://localhost:8000/admin/tables/certifications \
  -H "Authorization: Bearer YOUR_ADMIN_SECRET"
```

---

## User Endpoints (Modified)

These endpoints now respect certification status:

### Get Silver Catalog

**Endpoint:** `GET /api/data/catalog/silver`

**Authentication:** API Key required

**Response includes certification status:**
```json
{
  "tables": [
    {
      "name": "dim_commune",
      "actual_table_name": "dim_commune",
      "description_fr": "Table de dimension des communes...",
      "dependencies": ["bronze_geo"],
      "version": 1,
      "row_count": 34935,
      "certified": true,
      "certified_at": "2025-12-05T15:30:00.000Z",
      "certified_by": "admin"
    }
  ]
}
```

---

### Get Silver Table Detail

**Endpoint:** `GET /api/data/catalog/silver/{table_name}`

**Authentication:** API Key required

**Response includes certification status:**
```json
{
  "name": "dim_commune",
  "description_fr": "Table de dimension des communes...",
  "dependencies": ["bronze_geo"],
  "schema": { ... },
  "preview": [ ... ],
  "certified": true,
  "certified_at": "2025-12-05T15:30:00.000Z",
  "certified_by": "admin"
}
```

---

### Preview Table Data

**Endpoint:** `POST /api/data/preview/{layer}/{table_name}`

**Authentication:** API Key required

**Access Control:**
- Regular users: Only certified silver tables
- Admins: All tables in any layer

**Error Response (403 Forbidden):**
```json
{
  "detail": "Table dim_commune is not certified for public use. Please contact an administrator."
}
```

---

### Execute SQL Query

**Endpoint:** `POST /api/data/query`

**Authentication:** API Key required

**Access Control:**
- Regular users: Only certified silver tables are registered
- Admins: All tables are registered

**Request:**
```json
{
  "sql": "SELECT * FROM silver_dim_commune LIMIT 10",
  "limit": 1000
}
```

**Error Response (400 Bad Request) - Uncertified Table:**
```json
{
  "detail": "Table not found. Available tables: silver_dim_commune, silver_fact_loyer_annonce. Use format: layer_table_name (e.g., bronze_accueillants, silver_geo)"
}
```

**Note:** Uncertified tables simply won't be registered for non-admin users, so they appear as "not found" in queries.

---

## Error Codes

### 401 Unauthorized
- Missing or invalid API key/admin secret

### 403 Forbidden
- Attempting to access uncertified table as non-admin
- Attempting to access bronze/gold tables as non-admin

### 404 Not Found
- Table does not exist in registry

### 500 Internal Server Error
- Database connection issues
- PostgreSQL/database errors
- Unexpected errors

---

## Certification Workflow

### Recommended Process

1. **Develop and test table** in silver layer
2. **Validate data quality** using admin access
3. **Certify table** via admin API or UI
4. **Notify users** that table is available
5. **Monitor usage** and data quality
6. **Recertify** after major schema changes

### Best Practices

- Only certify tables that are stable and well-tested
- Document certified tables in your data catalog
- Review certifications periodically
- Uncertify tables before major changes
- Re-certify after changes are complete and validated

---

## Python Client Example

```python
import requests

# Configuration
BASE_URL = "https://odace.services.d4g.fr"  # or http://localhost:8080 for local
ADMIN_SECRET = "your_admin_secret"
API_KEY = "your_api_key"

# Admin: Certify a table
def certify_table(table_name, layer="silver"):
    response = requests.post(
        f"{BASE_URL}/admin/tables/certify",
        headers={
            "Authorization": f"Bearer {ADMIN_SECRET}",
            "Content-Type": "application/json"
        },
        json={
            "table_name": table_name,
            "layer": layer
        }
    )
    return response.json()

# User: Query certified tables
def query_table(sql):
    response = requests.post(
        f"{BASE_URL}/api/data/query",
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        },
        json={
            "sql": sql,
            "limit": 1000
        }
    )
    return response.json()

# Example usage
if __name__ == "__main__":
    # Certify dim_commune as admin
    result = certify_table("dim_commune")
    print(f"Certified: {result}")
    
    # Query as regular user
    data = query_table("SELECT * FROM silver_dim_commune LIMIT 10")
    print(f"Query result: {len(data['data'])} rows")
```


