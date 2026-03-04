# Security Migration Guide

**Purpose**: Step-by-step guide to safely deploy the security improvements to your production environment.

## Pre-Deployment Steps

### Step 1: Generate Strong Admin Secret

```bash
# Generate a cryptographically secure admin secret
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

**Save this output securely** - you'll need it for the next steps.

Example output:
```
a7Kx9mP2vB5nQ8wR3tY6uI0oL4jH1gF7dS9aZ2xC5vB8nM3
```

---

### Step 2: Update Local .env File

Create or update your `.env` file:

```bash
# Copy the template if you don't have a .env file
cp env.template .env

# Edit the .env file
nano .env  # or use your preferred editor
```

Required changes in `.env`:

```bash
# Storage & DB (see env.template: SCW_*, PG_DB_* or DATABASE_URL)

# API Configuration - CRITICAL: Replace with your generated secret
ADMIN_SECRET=<generate-with-python-secrets-module>

# Environment
ENVIRONMENT=development

# CORS Configuration
# For development, you can use wildcard:
CORS_ORIGINS=*

# For production, specify allowed domains (comma-separated):
# CORS_ORIGINS=https://yourdomain.com
```

---

### Step 3: Test Locally

```bash
# Activate virtual environment
source venv/bin/activate

# Install dependencies (if needed)
pip install -r requirements.txt

# Test that the application starts successfully
python -m uvicorn app.main:app --host 0.0.0.0 --port 8080
```

**Expected Output:**
```
INFO:     Starting Odace Data Pipeline API
INFO:     Environment: development
INFO:     Application startup complete.
```

**Test Authentication:**
```bash
# Test with your new admin secret
curl -H "Authorization: Bearer YOUR_ADMIN_SECRET" \
     https://odace.services.d4g.fr/api/pipeline/list
```

If you see an error about insecure admin secret, you need to use a different value.

---

## Production Deployment

### Step 4: Set secrets and environment

Set in your deployment platform (e.g. Coolify environment variables):

- `ADMIN_SECRET` (generated securely)
- `ENVIRONMENT=production`
- `CORS_ORIGINS=https://yourdomain.com` (your actual domain(s))
- Scaleway S3 and PostgreSQL vars (see env.template)

---

### Step 5: Deploy

Deploy via Docker/Coolify (see README).

**Watch for startup errors:**
```bash
# Check your deployment logs (e.g. Coolify dashboard)
```

---

### Step 6: Verify Production Deployment

**Test Health Endpoint:**
```bash
# Use your deployment URL (e.g. from Coolify)
SERVICE_URL="https://odace.services.d4g.fr"
curl $SERVICE_URL/health
```

**Expected Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-12-05T...",
  "version": "1.0.0"
}
```

**Test Authentication:**
```bash
# Test with admin secret (replace with your actual secret)
curl -H "Authorization: Bearer YOUR_ADMIN_SECRET" \
     $SERVICE_URL/api/pipeline/list
```

**Test CORS:**
```bash
# Test that CORS is properly configured
curl -H "Origin: https://yourdomain.com" \
     -H "Access-Control-Request-Method: POST" \
     -H "Access-Control-Request-Headers: Authorization" \
     -X OPTIONS \
     -v \
     $SERVICE_URL/api/pipeline/list
```

Look for `Access-Control-Allow-Origin` header in the response.

---

## Rollback Procedure

If something goes wrong, you can rollback:

### Quick Rollback

Use your deployment platform's rollback (e.g. Coolify: revert to previous deployment).

### Emergency: Restore Old Secret

Update `ADMIN_SECRET` in your deployment environment to a previous value if you have it stored securely.

---

## Post-Deployment Verification

### Test Rate Limiting

**Test Pipeline Endpoint (10/hour limit):**
```bash
# This should succeed 10 times, then fail
for i in {1..12}; do
  echo "Request $i:"
  curl -X POST \
    -H "Authorization: Bearer YOUR_ADMIN_SECRET" \
    $SERVICE_URL/api/pipeline/run
  sleep 1
done
```

After 10 requests, you should see:
```json
{"detail":"Rate limit exceeded: 10 per 1 hour"}
```

### Test CORS Protection

**Test from unauthorized origin (should fail):**
```bash
curl -H "Origin: https://evil-site.com" \
     -H "Access-Control-Request-Method: GET" \
     -X OPTIONS \
     $SERVICE_URL/api/pipeline/list
```

Should NOT include `Access-Control-Allow-Origin: https://evil-site.com`

---

## Monitoring

### Set Up Alerts

Use your deployment platform's logging (e.g. Coolify logs) to monitor:

1. **Rate limit violations** – filter for "Rate limit exceeded"
2. **Authentication failures** – filter for "Invalid admin secret" or "Invalid API key"
3. **Warnings and errors** – review regularly

---

## Common Issues and Solutions

### Issue 1: Application Won't Start

**Error:** `ValueError: ADMIN_SECRET is set to an insecure value`

**Solution:** 
- Generate a new secret using the Python command in Step 1
- Ensure it's not a common word or simple value
- Update both local `.env` and deployment environment (e.g. Coolify env vars)

---

### Issue 2: CORS Errors in Production

**Error:** Browser shows CORS policy errors

**Solution:** Update `CORS_ORIGINS` in your deployment environment (e.g. Coolify env vars) to include your front-end domain(s).

---

### Issue 3: Rate Limit Too Restrictive

**Temporary Solution:**
Update rate limits in code if needed (not recommended for security):
- Edit `app/api/routes/*.py`
- Increase limits for specific endpoints
- Redeploy

**Better Solution:**
- Implement request batching in your client code
- Use caching to reduce API calls
- Request rate limit increase with justification

---

### Issue 4: Lost Admin Secret

**Recovery:**
1. Generate a new secret: `python3 -c "import secrets; print(secrets.token_urlsafe(32))"`
2. Set the new value in your deployment environment (e.g. Coolify env vars)
3. Restart/redeploy the application
4. Update your local `.env` file
5. **Revoke all API keys** if admin access was compromised
6. Create new API keys for users via Admin API

---

## Security Checklist

Before marking deployment as complete:

- [ ] Generated strong admin secret (32+ characters, random)
- [ ] Set ADMIN_SECRET in deployment environment
- [ ] Set CORS_ORIGINS to specific domains (no wildcard in production)
- [ ] Set ENVIRONMENT=production
- [ ] Tested authentication with new admin secret
- [ ] Verified CORS headers in production
- [ ] Tested rate limiting behavior
- [ ] Set up monitoring and alerts
- [ ] Updated local .env file
- [ ] Documented the new admin secret securely (password manager)
- [ ] Tested rollback procedure
- [ ] Notified team of new authentication requirements

---

## Next Steps

After successful deployment:

1. **Update Documentation:**
   - Update API documentation with new rate limits
   - Document new CORS requirements for frontend developers
   - Update admin onboarding guide with new security requirements

2. **Train Team:**
   - Brief team on new security measures
   - Share admin secret securely (use password manager)
   - Explain rate limiting impacts

3. **Plan Next Phase:**
   - Review remaining security recommendations in `SECURITY_IMPROVEMENTS.md`
   - Schedule implementation of short-term improvements
   - Plan for dependency updates

---

## Support

If you encounter issues during migration:

1. Check the detailed error messages in your deployment logs
2. Review the `SECURITY_IMPROVEMENTS.md` documentation
3. Test locally first before deploying to production
4. Keep the rollback procedure handy

**Emergency:** Use your deployment platform's dashboard for rollback or env updates.


