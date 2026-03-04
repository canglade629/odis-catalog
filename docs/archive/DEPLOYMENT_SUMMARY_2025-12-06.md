# Deployment Summary - December 6, 2025

## Overview
Successfully deployed updated SIAE pipeline to Cloud Run and executed the full data pipeline.

## Deployment Details

### Service Information
- **Service Name**: odace-pipeline
- **Service URL**: https://odace-pipeline-588398598428.europe-west1.run.app
- **Region**: europe-west1
- **Project**: icc-project-472009
- **Revision**: odace-pipeline-00164-r77

### Configuration
- **Memory**: 2Gi
- **CPU**: 2 cores
- **Timeout**: 300 seconds
- **Max Instances**: 10
- **Environment**: production

## Pipeline Execution Results

### 1. Bronze Layer - SIAE Structures
**Job ID**: 4aa6486d-32f5-4695-8378-f88117e6ba40

**Status**: ✅ SUCCESS

**Statistics**:
- Files Processed: 1
- Rows Processed: 86,374
- Source File: `gs://jaccueille/raw/api/siae_structures/structures-inclusion-2025-12-01.csv`
- Target Table: `bronze_siae_structures`

**Key Changes**:
- Updated pipeline to read CSV format (previously JSON)
- Added explicit dtype specifications to ensure proper string handling for SIRET and other ID fields
- Properly handles geographic coordinates as numeric values

### 2. Silver Layer - dim_siae_structure
**Job ID**: 5d705c6c-4001-419f-bf9d-27f77887d971

**Status**: ✅ SUCCESS

**Statistics**:
- Rows Processed: 35,119
- Target Table: `silver_dim_siae_structure`
- Deduplication: 86,374 → 35,119 rows (59% reduction after SIRET deduplication)

**Key Changes**:
- Updated column mappings to match new CSV structure
- New primary key: `MD5(id)` instead of `MD5(siret)`
- Added business key: `siae_structure_bk`
- Renamed columns to match naming conventions:
  - `siae_structure_siret_code` (from `siret`)
  - `siae_structure_label` (from `nom`)
  - `siae_structure_description` (from `description`)
- Foreign key to `dim_commune` via `MD5(commune)`

## Code Changes

### Files Modified
1. `app/pipelines/bronze/siae_structures.py` - CSV reading with proper dtype handling
2. `app/pipelines/silver/dim_siae_structure.py` - Updated SQL mapping
3. `app/pipelines/silver/fact_siae_poste.py` - Updated join to use new column name
4. `config/data_catalogue.yaml` - Updated field definitions
5. `app/core/config.py` - Added `extra = "ignore"` for environment variables

### Data Quality
- **Bronze**: 86,374 structures from nationwide SIAE directory
- **Silver**: 35,119 unique structures (deduplicated by SIRET)
- **Data Sources**: ma-boussole-aidants and other inclusion platforms
- **Geographic Coverage**: Full France with INSEE codes and coordinates

## Next Steps
- Monitor pipeline performance in Cloud Run logs
- Consider updating fact_siae_poste pipeline if needed
- Update any downstream dependencies that reference dim_siae_structure

## Admin Access
- Admin Secret: Stored in Google Secret Manager as `odace-admin-secret`
- API Endpoint Pattern: `https://odace-pipeline-588398598428.europe-west1.run.app/api/{layer}/{pipeline_name}`
- Authentication: Bearer token in Authorization header

## Verification
All pipelines executed successfully with no errors. Data is now available in the silver layer for analysis and downstream consumption.


