# Commune Join Quality Report

**Generated**: 2025-12-06 16:53:37 UTC  
**Database**: Cloud Run DuckDB Instance  
**Reference Table**: `silver_dim_commune` (34935 communes)

## Executive Summary

This report validates foreign key join quality between silver layer tables and the `dim_commune` dimension table via the `commune_sk` surrogate key.

## Overview

| Table | Total Rows | With FK | Successful Joins | Orphaned FKs | Match Rate | Population Rate |
|-------|------------|---------|------------------|--------------|------------|----------------|
| dim_accueillant | 1,863 | 397 | 397 | 0 | 100.00% | 21.31% |
| dim_gare | 2,785 | 2,785 | 2,540 | 245 | 91.20% | 100.00% |
| dim_siae_structure | 35,119 | 34,893 | 1 | 34,892 | 0.00% | 99.36% |
| fact_loyer_annonce | 139,672 | 139,672 | 139,672 | 0 | 100.00% | 100.00% |
| fact_zone_attraction | 26,209 | 26,209 | 26,209 | 0 | 100.00% | 100.00% |
| **TOTAL** | **205,648** | **203,956** | **168,819** | **35,137** | **82.77%** | **99.18%** |

## Detailed Analysis

### dim_accueillant

**Description**: Accueillants dimension - joins via postal code (first 5 chars)  
**FK Nullable**: True  

**Statistics**:
- Total records: 1,863
- Records with commune_sk: 397 (21.31%)
- Successful joins: 397
- Orphaned FKs: 0
- Match rate: 100.00%

**Status**: ✅ All foreign keys are valid

### dim_gare

**Description**: Train stations dimension - joins via commune code  
**FK Nullable**: True  

**Statistics**:
- Total records: 2,785
- Records with commune_sk: 2,785 (100.00%)
- Successful joins: 2,540
- Orphaned FKs: 245
- Match rate: 91.20%

**Status**: ❌ Significant issues - 245 orphaned records (8.80%)

**Orphaned FK Sample** (top 5 by frequency):
- ``: 245 records

### dim_siae_structure

**Description**: SIAE structures dimension - joins via MD5(commune name)  
**FK Nullable**: True  

**Statistics**:
- Total records: 35,119
- Records with commune_sk: 34,893 (99.36%)
- Successful joins: 1
- Orphaned FKs: 34,892
- Match rate: 0.00%

**Status**: ❌ Significant issues - 34,892 orphaned records (100.00%)

**Orphaned FK Sample** (top 5 by frequency):
- `e20d37a5d7fcc4c35be6fc18a8e71bfa`: 880 records
- `1c05d905c9d6f70f754a6846d86b0070`: 523 records
- `8377de0f1845174610bd8b815b9a285e`: 357 records
- `66d73a5c118ed4653630c2fe3464cf2d`: 288 records
- `debecab907ccabee0bc672b9fa1cab8a`: 227 records

### fact_loyer_annonce

**Description**: Rental price facts - direct join  
**FK Nullable**: False  

**Statistics**:
- Total records: 139,672
- Records with commune_sk: 139,672 (100.00%)
- Successful joins: 139,672
- Orphaned FKs: 0
- Match rate: 100.00%

**Status**: ✅ All foreign keys are valid

### fact_zone_attraction

**Description**: Attraction zone facts - joins via CODGEO  
**FK Nullable**: False  

**Statistics**:
- Total records: 26,209
- Records with commune_sk: 26,209 (100.00%)
- Successful joins: 26,209
- Orphaned FKs: 0
- Match rate: 100.00%

**Status**: ✅ All foreign keys are valid

## Issues and Recommendations

### dim_gare

**Issue**: 245 orphaned foreign keys (8.80% of non-null FKs)

**Recommendations**:
- Review commune code enrichment logic
- Validate source commune codes against INSEE reference

### dim_siae_structure

**Issue**: 34,892 orphaned foreign keys (100.00% of non-null FKs)

**Recommendations**:
- Review commune name matching logic (currently using MD5(commune))
- Consider fuzzy matching or standardization for commune names
- Verify that commune names in source data match dim_commune entries

## Conclusion

Out of 203,956 records with commune_sk foreign keys across all tables:
- 168,819 (82.77%) successfully join to dim_commune
- 35,137 (17.23%) are orphaned (invalid FKs)

**Overall Assessment**: ❌ Poor - Significant data quality issues require attention
