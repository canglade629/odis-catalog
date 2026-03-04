# Security Quick Reference Card

## 🔐 Critical Security Changes - Quick Reference

---

## 1. Admin Secret (REQUIRED)

### Before:
```python
admin_secret: str = "changeme"  # ❌ Insecure default
```

### After:
```python
admin_secret: str  # ✅ No default, must be explicitly set
```

### Action Required:
```bash
# Generate strong secret
python3 -c "import secrets; print(secrets.token_urlsafe(32))"

# Add to .env
ADMIN_SECRET=<generated-secret-here>
```

**Application will refuse to start without a valid admin secret!**

---

## 2. CORS Configuration

### Before:
```python
allow_origins=["*"]  # ❌ Allows any domain
```

### After:
```python
allow_origins=settings.allowed_origins  # ✅ Configured per environment
```

### Configuration:

**Development:**
```bash
ENVIRONMENT=development
CORS_ORIGINS=*  # Wildcard allowed in dev
```

**Production:**
```bash
ENVIRONMENT=production
CORS_ORIGINS=https://yourdomain.com,https://app.yourdomain.com  # Specific domains required
```

**Application will refuse to start if CORS wildcard used in production!**

---

## 3. Rate Limits

### New Rate Limits by Endpoint Type:

| Endpoint Type | Old Limit | New Limit | Example Endpoints |
|---------------|-----------|-----------|-------------------|
| Pipeline Execution | 60/min | **10/hour** | `/api/bronze/*`, `/api/silver/*`, `/api/pipeline/run` |
| File Upload | 60/min | **20/hour** | `/api/files/upload` |
| Admin Operations | None | **100/hour** | `/admin/api-keys`, `/admin/tables/certify` |
| Catalogue Refresh | 10/min | **10/hour** | `/admin/catalogue/refresh` |
| Data Queries | 60/min | **60/min** ✓ | `/api/data/query`, `/api/data/preview` |
| Read Operations | 60/min | **60/min** ✓ | `/api/jobs`, `/api/pipeline/list` |

### Error Response:
```json
{
  "detail": "Rate limit exceeded: 10 per 1 hour"
}
```

---

## 4. Timing-Safe Comparison

### Before:
```python
if credentials.credentials != settings.admin_secret:  # ❌ Vulnerable to timing attacks
```

### After:
```python
if not secrets.compare_digest(credentials.credentials, settings.admin_secret):  # ✅ Constant-time comparison
```

**Protects against timing side-channel attacks on admin authentication.**

---

## 📋 Required Environment Variables

### Minimal .env Configuration:

```bash
# Storage & DB (see env.template)
# SCW_* for Scaleway S3, PG_DB_* or DATABASE_URL for PostgreSQL

# Security (required)
ADMIN_SECRET=<generate-with-secrets-module>
ENVIRONMENT=development|production

# CORS (required for production)
CORS_ORIGINS=https://yourdomain.com
```

---

## 🚀 Quick Deploy Commands

### Local Testing:
```bash
# 1. Update .env file with strong ADMIN_SECRET
# 2. Start application
python -m uvicorn app.main:app --host 0.0.0.0 --port 8080

# 3. Test authentication
curl -H "Authorization: Bearer YOUR_ADMIN_SECRET" \
     https://odace.services.d4g.fr/api/pipeline/list
```

### Production Deploy:
```bash
# 1. Set ADMIN_SECRET (and DB/S3 vars) in your deployment platform (e.g. Coolify env vars)
# 2. Set CORS_ORIGINS to your front-end domain(s)
# 3. Deploy via Docker/Coolify (see README)
```

---

## ⚠️ Breaking Changes

### 1. Admin Secret Required
- Application will not start without valid `ADMIN_SECRET`
- Default value "changeme" is now rejected
- Must use cryptographically secure random value

### 2. CORS Restrictions
- Wildcard `*` only allowed in `ENVIRONMENT=development`
- Production requires specific allowed origins
- Application will not start with wildcard in production

### 3. Rate Limiting
- Pipeline execution limited to 10/hour (was 60/minute)
- File uploads limited to 20/hour (was 60/minute)
- May affect automation scripts and batch operations

---

## 🔍 Verification Commands

### Check Application Started Successfully:
```bash
# Local
curl http://localhost:8080/health

# Production
curl https://odace.services.d4g.fr/health
```

Expected:
```json
{"status":"healthy","timestamp":"...","version":"1.0.0"}
```

### Test Authentication:
```bash
# Should succeed
curl -H "Authorization: Bearer YOUR_ADMIN_SECRET" \
     $SERVICE_URL/api/pipeline/list

# Should fail
curl -H "Authorization: Bearer wrong-secret" \
     $SERVICE_URL/api/pipeline/list
```

### Test Rate Limiting:
```bash
# Make 11 requests quickly (should fail on 11th)
for i in {1..11}; do
  curl -X POST -H "Authorization: Bearer YOUR_ADMIN_SECRET" \
       $SERVICE_URL/api/pipeline/run
done
```

### Test CORS:
```bash
# From allowed origin - should include Access-Control-Allow-Origin header
curl -H "Origin: https://yourdomain.com" -v \
     $SERVICE_URL/api/pipeline/list
```

---

## 🐛 Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| App won't start: "insecure value" | Weak admin secret | Generate new: `python3 -c "import secrets; print(secrets.token_urlsafe(32))"` |
| App won't start: "CORS wildcard" | `CORS_ORIGINS=*` in production | Set specific domains or change to `ENVIRONMENT=development` |
| "Rate limit exceeded" | Too many requests | Wait for rate limit window to reset, or cache responses |
| CORS errors in browser | Origin not in CORS_ORIGINS | Add your domain to CORS_ORIGINS env var |
| "Invalid admin secret" | Wrong secret in request | Check ADMIN_SECRET in .env matches request header |

---

## 📊 Monitoring

### Watch for Security Events

Use your deployment platform's log viewer (e.g. Coolify logs) to filter for:
- Authentication failures: `Invalid admin secret`, `Invalid API key`
- Rate limit violations: `Rate limit exceeded`
- Startup: `ADMIN_SECRET`, `CORS`

---

## 📚 Documentation Links

- Full Implementation Details: `SECURITY_IMPROVEMENTS.md`
- Step-by-Step Migration: `SECURITY_MIGRATION_GUIDE.md`
- Complete Security Review: `secur.plan.md`

---

## ✅ Pre-Deployment Checklist

Before deploying to production:

- [ ] Generated strong admin secret (32+ random characters)
- [ ] Updated `.env` with new ADMIN_SECRET
- [ ] Set CORS_ORIGINS to specific domains (no wildcard)
- [ ] Set ENVIRONMENT=production
- [ ] Set ADMIN_SECRET in deployment environment
- [ ] Tested locally first
- [ ] Reviewed rate limits vs. usage patterns
- [ ] Prepared rollback plan
- [ ] Notified team of changes
- [ ] Updated API documentation

---

## 🎯 Quick Win Testing

**5-Minute Security Test:**

```bash
# Set variables
export ADMIN_SECRET="your-secret-here"
export SERVICE_URL="https://odace.services.d4g.fr"  # or http://localhost:8080 for local

# 1. Health check
curl $SERVICE_URL/health && echo "✅ App is running"

# 2. Auth works
curl -H "Authorization: Bearer $ADMIN_SECRET" \
     $SERVICE_URL/api/pipeline/list && echo "✅ Auth working"

# 3. Auth fails correctly
curl -H "Authorization: Bearer wrong" \
     $SERVICE_URL/api/pipeline/list && echo "❌ Expected to fail"

# 4. Rate limit exists
for i in {1..11}; do
  curl -X POST -H "Authorization: Bearer $ADMIN_SECRET" \
       $SERVICE_URL/api/pipeline/run 2>/dev/null | grep -q "Rate limit" && echo "✅ Rate limit working" && break
done
```

If all checks pass: **✅ Security improvements successfully deployed!**


