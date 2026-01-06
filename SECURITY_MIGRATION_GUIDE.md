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
# GCP Configuration
GCP_PROJECT_ID=icc-project-472009
GCS_BUCKET=jaccueille
GCS_RAW_PREFIX=raw
GCS_DELTA_PREFIX=delta

# API Configuration - CRITICAL: Replace with your generated secret
ADMIN_SECRET=a7Kx9mP2vB5nQ8wR3tY6uI0oL4jH1gF7dS9aZ2xC5vB8nM3

# Environment
ENVIRONMENT=development

# CORS Configuration
# For development, you can use wildcard:
CORS_ORIGINS=*

# For production, specify allowed domains (comma-separated):
# CORS_ORIGINS=https://odace-pipeline-588398598428.europe-west1.run.app,https://yourdomain.com
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
INFO:     GCS Bucket: jaccueille
INFO:     Project: icc-project-472009
INFO:     Application startup complete.
```

**Test Authentication:**
```bash
# Test with your new admin secret
curl -H "Authorization: Bearer YOUR_ADMIN_SECRET" \
     http://localhost:8080/api/pipeline/list
```

If you see an error about insecure admin secret, you need to use a different value.

---

## Production Deployment

### Step 4: Update GCP Secret Manager

```bash
# Set your project
gcloud config set project icc-project-472009

# Create/update the admin secret in Secret Manager
echo -n "a7Kx9mP2vB5nQ8wR3tY6uI0oL4jH1gF7dS9aZ2xC5vB8nM3" | \
gcloud secrets versions add odace-admin-secret --data-file=-
```

**Verify the secret was created:**
```bash
gcloud secrets versions list odace-admin-secret
```

---

### Step 5: Update Environment Variables for Production

Edit your `deploy.sh` or Cloud Run configuration to include:

```bash
# In deploy.sh, update the --set-env-vars line:
--set-env-vars "ENVIRONMENT=production,GCP_PROJECT_ID=icc-project-472009,GCS_BUCKET=jaccueille,CORS_ORIGINS=https://odace-pipeline-588398598428.europe-west1.run.app"
```

**Important:** Replace the CORS_ORIGINS value with your actual production domain(s).

---

### Step 6: Deploy to Cloud Run

```bash
# Make deploy script executable
chmod +x deploy.sh

# Run deployment
./deploy.sh
```

**Watch for startup errors:**
```bash
# Check Cloud Run logs
gcloud run logs read odace-pipeline --region europe-west1 --limit 50
```

---

### Step 7: Verify Production Deployment

**Test Health Endpoint:**
```bash
# Get your Cloud Run URL
SERVICE_URL=$(gcloud run services describe odace-pipeline \
    --region europe-west1 \
    --format 'value(status.url)')

# Test health endpoint
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

```bash
# List recent revisions
gcloud run revisions list --service odace-pipeline --region europe-west1

# Rollback to previous revision
gcloud run services update-traffic odace-pipeline \
    --region europe-west1 \
    --to-revisions PREVIOUS_REVISION=100
```

### Emergency: Restore Old Secret

```bash
# List previous secret versions
gcloud secrets versions list odace-admin-secret

# Restore a previous version (use version number from above)
gcloud secrets versions enable VERSION_NUMBER --secret odace-admin-secret
```

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

Monitor for these security events:

1. **Rate Limit Violations:**
```bash
gcloud logging metrics create rate_limit_violations \
    --description="Rate limit exceeded events" \
    --log-filter='resource.type="cloud_run_revision"
    AND textPayload=~"Rate limit exceeded"'
```

2. **Authentication Failures:**
```bash
gcloud logging metrics create auth_failures \
    --description="Authentication failure events" \
    --log-filter='resource.type="cloud_run_revision"
    AND (textPayload=~"Invalid admin secret" OR textPayload=~"Invalid API key")'
```

### Check Logs Regularly

```bash
# Check for security-related logs
gcloud run logs read odace-pipeline \
    --region europe-west1 \
    --filter="severity>=WARNING" \
    --limit 50
```

---

## Common Issues and Solutions

### Issue 1: Application Won't Start

**Error:** `ValueError: ADMIN_SECRET is set to an insecure value`

**Solution:** 
- Generate a new secret using the Python command in Step 1
- Ensure it's not a common word or simple value
- Update both local `.env` and GCP Secret Manager

---

### Issue 2: CORS Errors in Production

**Error:** Browser shows CORS policy errors

**Solution:**
```bash
# Check current CORS_ORIGINS setting
gcloud run services describe odace-pipeline \
    --region europe-west1 \
    --format='value(spec.template.spec.containers[0].env)'

# Update CORS_ORIGINS
gcloud run services update odace-pipeline \
    --region europe-west1 \
    --update-env-vars CORS_ORIGINS=https://your-domain.com
```

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
2. Update Secret Manager: `echo -n "NEW_SECRET" | gcloud secrets versions add odace-admin-secret --data-file=-`
3. Restart Cloud Run: `gcloud run services update odace-pipeline --region europe-west1`
4. Update your local `.env` file
5. **Revoke all API keys** if admin access was compromised
6. Create new API keys for users

---

## Security Checklist

Before marking deployment as complete:

- [ ] Generated strong admin secret (32+ characters, random)
- [ ] Updated GCP Secret Manager with new secret
- [ ] Set CORS_ORIGINS to specific domains (no wildcard in production)
- [ ] Set ENVIRONMENT=production in Cloud Run
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

1. Check the detailed error messages in Cloud Run logs
2. Review the `SECURITY_IMPROVEMENTS.md` documentation
3. Test locally first before deploying to production
4. Keep the rollback procedure handy

**Emergency Contact:** Maintain access to GCP Console for manual intervention if needed.


