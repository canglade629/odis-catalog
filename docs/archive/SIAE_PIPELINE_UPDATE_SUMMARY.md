# SIAE Pipeline Update Summary

## Date: December 6, 2025

## Overview
Updated the SIAE structures pipeline to work with the new CSV data source format available in `raw/api/siae_structures/structures-inclusion-2025-12-01.csv`.

## Changes Made

### 1. Bronze Pipeline (`app/pipelines/bronze/siae_structures.py`)
- **Changed**: Updated to read CSV files instead of JSON files
- **Modified**: File reading logic now uses `pd.read_csv()` instead of `json.loads()`
- **Updated**: Timestamp extraction logic to handle CSV filename format (`YYYY-MM-DD`)

### 2. Silver Pipeline (`app/pipelines/silver/dim_siae_structure.py`)
- **Updated**: Column mapping to match new CSV structure based on provided table
- **Key changes**:
  - `siae_structure_sk` now uses `MD5(id)` instead of `MD5(siret)`
  - Added `siae_structure_bk` (business key) mapped from `id`
  - Added `siae_structure_siret_code` mapped from `siret`
  - Added `siae_structure_label` mapped from `nom`
  - Added `siae_structure_description` mapped from `description`
  - Changed `commune_sk` to use `MD5(commune)` for simplicity
  - Removed obsolete columns: `raison_sociale`, `enseigne`, `structure_type`, `accepte_candidatures`, `date_creation`, `date_mise_a_jour`, `departement`, `ville`, `adresse_ligne1`, `adresse_ligne2`, `ville_standardisee`
  - Kept: `code_postal`, `adresse`, `complement_adresse`, `longitude`, `latitude`, `telephone`, `courriel`, `site_web`

### 3. Fact Pipeline (`app/pipelines/silver/fact_siae_poste.py`)
- **Updated**: Join condition to use `siae_structure_bk` instead of `id`

### 4. Data Catalogue (`config/data_catalogue.yaml`)
- **Updated**: `dim_siae_structure` field definitions to match new structure
- **Updated**: Row count from 1,976 to 86,374 (based on new data)

### 5. Configuration (`app/core/config.py`)
- **Fixed**: Added `extra = "ignore"` to Config class to handle extra environment variables

## New CSV Data Structure

The CSV file contains the following columns:
- `id` - Unique identifier (e.g., "ma-boussole-aidants--10073851")
- `siret` - SIRET number (14 digits)
- `nom` - Structure name
- `commune` - Municipality name
- `code_postal` - Postal code
- `code_insee` - INSEE code
- `adresse` - Main address
- `complement_adresse` - Address complement
- `longitude` - Longitude coordinate
- `latitude` - Latitude coordinate
- `telephone` - Phone number
- `courriel` - Email address
- `site_web` - Website URL
- `description` - Structure description
- `source` - Data source
- `date_maj` - Last update date
- `lien_source` - Source link
- `horaires_accueil` - Opening hours
- `accessibilite_lieu` - Accessibility information
- `reseaux_porteurs` - Supporting networks

## Testing
All pipelines have been validated and can be successfully instantiated without errors.

## Column Mapping Table

| Silver Column | Source Column |
|---------------|---------------|
| siae_structure_SK | hash(id) |
| siae_structure_bk | id |
| siae_structure_siret_code | siret |
| siae_structure_label | nom |
| siae_structure_description | description |
| commune_sk | MD5(commune) |
| code_postal | code_postal |
| adresse | adresse |
| complement_adresse | complement_adresse |
| longitude | longitude |
| latitude | latitude |
| telephone | telephone |
| courriel | courriel |
| site_web | site_web |


