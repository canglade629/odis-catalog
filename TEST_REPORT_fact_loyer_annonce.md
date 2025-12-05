# Test Execution Report: fact_loyer_annonce Pipeline

**Generated:** 2025-12-05T10:27:24.302086

```
================================================================================
TEST EXECUTION REPORT - fact_loyer_annonce Pipeline
================================================================================
Start Time: 2025-12-05T10:27:00.679681
End Time: 2025-12-05T10:27:24.302086
Total Duration: 23.62s

================================================================================
SUMMARY
================================================================================
Test Suites: 2/5 passed
Individual Tests: 27/39 passed
  ✅ Passed: 27
  ❌ Failed: 12
  ⊘ Skipped: 0

================================================================================
DETAILED RESULTS
================================================================================

1. SQL Syntax Validation
   Status: ✅ PASS
   File: sql_validation_fact_loyer_annonce.py
   Duration: 3.20s
   Tests: 3 passed, 0 failed, 0 skipped

2. Bronze Layer Unit Tests
   Status: ❌ FAIL
   File: test_bronze_logement.py
   Duration: 2.73s
   Tests: 14 passed, 1 failed, 0 skipped

3. Silver Layer Unit Tests
   Status: ❌ FAIL
   File: test_fact_loyer_annonce.py
   Duration: 10.50s
   Tests: 0 passed, 4 failed, 0 skipped

4. Integration Tests
   Status: ❌ FAIL
   File: test_integration.py
   Duration: 4.96s
   Tests: 0 passed, 7 failed, 0 skipped

5. Data Quality Validation
   Status: ✅ PASS
   File: test_data_quality_loyer.py
   Duration: 2.24s
   Tests: 10 passed, 0 failed, 0 skipped

================================================================================
❌ SOME TESTS FAILED
================================================================================

Please review failed tests above and fix issues before deployment.

```
