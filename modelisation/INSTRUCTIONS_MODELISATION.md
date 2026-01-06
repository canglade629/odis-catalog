# Instructions de Mise à Jour de la Modélisation - ODACE Platform

**Date:** 2025-12-01  
**Destinataires:** Équipe de développement  
**Sujet:** Intégration des bonnes pratiques de modélisation Silver Layer

---

## 📋 Contexte

Ce document contient les instructions pour mettre à jour la modélisation existante des tables Bronze et Silver afin d'intégrer les bonnes pratiques de l'architecture Médaillon et de garantir la cohérence avec les standards du projet ODACE.

> **IMPORTANT**: Ce document applique les conventions de nommage définies dans `NAMING_CONVENTIONS.md` :
> - **Français** pour les entités du domaine uniquement (loyer, commune, logement)
> - **Anglais** pour tout le reste (attributs, opérations, transformations, technique)

**Références:** 
- `modelisation_draft.md` pour les spécifications détaillées
- `NAMING_CONVENTIONS.md` pour les règles de nommage

---

## 🎯 PRINCIPES FONDAMENTAUX DE MODÉLISATION

### Règles de Normalisation et Conception Silver Layer

Ces principes **DOIVENT** être appliqués à toutes les tables Silver pour garantir la qualité, la maintenabilité et l'utilisabilité des données.

#### 1️⃣ Une table = une entité logique

**Règle:** Chaque table doit représenter un seul concept bien défini.

- ✅ `dim_commune` représente uniquement les communes
- ✅ `dim_gare` représente uniquement les gares
- ❌ Une table `lieux` mélangeant gares, communes et structures serait incorrecte

**Application:**
- Identifiez clairement le concept métier de chaque table
- Si une table contient des informations sur plusieurs entités distinctes, séparez-la
- Nommez vos tables selon l'entité qu'elles représentent

---

#### 2️⃣ Une clé = une identité

**Règle:** La clé primaire identifie l'entité de manière unique et ne représente rien d'autre.

- ✅ `commune_sk` identifie une commune unique
- ✅ `gare_sk` identifie une gare unique
- ❌ Une clé composite `(commune_sk, annee)` identifierait une commune-année (= entité différente)

**Application:**
- Utilisez une clé de substitution simple (`_sk`) pour chaque dimension et fait
- La clé primaire doit identifier l'entité complète, pas une version partielle ou temporelle
- Pour les tables de faits avec plusieurs dimensions, la clé primaire peut être une combinaison de clés étrangères

---

#### 3️⃣ Les colonnes décrivent la clé — rien de plus

**Règle:** Chaque colonne doit décrire directement l'entité identifiée par la clé. Aucune colonne ne doit dépendre d'une autre colonne non-clé.

**✅ Correct (3NF):**
```sql
-- dim_commune
commune_sk          -- Clé primaire
commune_insee_code  -- Décrit la commune
commune_label       -- Décrit la commune
departement_code    -- Décrit la commune (dans quel département elle se trouve)
```

**❌ Incorrect (violation de 3NF):**
```sql
-- dim_commune (mauvais design)
commune_sk          -- Clé primaire
commune_insee_code
commune_label
departement_code    
departement_label   -- ❌ Dépend de departement_code, pas de commune_sk
region_code         
region_label        -- ❌ Dépend de region_code, pas de commune_sk
```

**Application:**
- Si une colonne décrit une autre colonne non-clé, elle appartient à une autre table
- Créez des tables de dimension séparées pour les hiérarchies (département, région)
- Utilisez des clés étrangères pour relier les entités

---

#### 4️⃣ Éviter les dépendances transitives

**Règle:** Les attributs doivent dépendre uniquement de la clé, pas d'autres attributs.

**Dépendance transitive:** A → B → C (où A est la clé)

**Exemple de problème:**
```sql
-- ❌ MAUVAIS: fact_loyer_annonce avec dépendances transitives
row_sk
commune_insee_code        -- Identifie la commune
commune_label             -- ❌ Dépend de commune_insee_code (transitive)
departement_code          -- ❌ Dépend de commune_insee_code (transitive)
departement_label         -- ❌ Dépend de departement_code (transitive)
loyer_m2_moy
```

**Solution (normalisé):**
```sql
-- ✅ CORRECT: fact_loyer_annonce normalisé
row_sk
commune_sk          -- FK vers dim_commune
loyer_m2_moy
-- Tous les autres attributs géographiques viennent de dim_commune via la FK
```

**Application:**
- Identifiez les chaînes de dépendances dans vos colonnes
- Déplacez les attributs transitifs vers des tables de dimension dédiées
- Utilisez des clés étrangères pour maintenir les relations

---

#### 5️⃣ Ne jamais dupliquer les informations stables

**Règle:** Tout attribut qui change rarement doit vivre dans une seule table de référence. Les autres tables ne doivent stocker que sa clé.

**Informations stables (à centraliser):**
- Libellés géographiques (commune, département, région)
- Codes officiels (INSEE, SIRET, ROME)
- Nomenclatures et référentiels (types de contrat, catégories)

**✅ Correct:**
```sql
-- dim_commune (table de référence unique)
commune_sk
commune_insee_code
commune_label       -- Stocké UNE SEULE FOIS
departement_code

-- fact_loyer_annonce
row_sk
commune_sk          -- ✅ Référence via FK
loyer_m2_moy

-- fact_zone_attraction
zone_attraction_sk
commune_sk          -- ✅ Même référence via FK
aire_attraction_code
```

**❌ Incorrect:**
```sql
-- fact_loyer_annonce
row_sk
commune_insee_code
commune_label       -- ❌ Dupliqué

-- fact_zone_attraction
zone_attraction_sk
commune_insee_code
commune_label       -- ❌ Dupliqué (risque d'incohérence)
```

**Avantages:**
- **Cohérence:** Une seule source de vérité pour chaque information
- **Maintenance:** Mise à jour en un seul endroit
- **Économie d'espace:** Pas de duplication massive
- **Intégrité:** Les FK garantissent la validité des références

**Application:**
- Créez des dimensions pour tous les référentiels stables
- Supprimez les colonnes dénormalisées des tables de faits
- Utilisez des jointures pour reconstituer les informations au besoin

---

#### 6️⃣ Chaque table Silver doit être utilisable seule

**Règle:** Pas de dépendances cachées, pas de jointures implicites. Les colonnes doivent être explicites, claires et immédiatement exploitables par les consommateurs.

**✅ Table complète et auto-suffisante:**
```sql
CREATE TABLE fact_loyer_commune (
    loyer_commune_sk            STRING,         -- Identifiant unique
    commune_sk                  STRING,         -- FK explicite
    epci_sk                     STRING,         -- FK explicite
    annee_sk                    STRING,         -- FK explicite (ou INTEGER annee)
    prediction_type_sk          STRING,         -- FK explicite
    
    -- Faits mesurables directement
    loyer_predicted_m2          DECIMAL(10,2),
    loyer_lower_bound_m2        DECIMAL(10,2),
    loyer_upper_bound_m2        DECIMAL(10,2),
    
    -- Indicateurs de qualité
    quality_r2_adjusted         DECIMAL(5,4),
    count_observations_grid     INTEGER,
    count_observations_commune  INTEGER,
    
    -- Métadonnées (traçabilité complète)
    job_insert_id               STRING,
    job_insert_date_utc         TIMESTAMP_NTZ,
    job_modify_id               STRING,
    job_modify_date_utc         TIMESTAMP_NTZ
);
```

**Caractéristiques d'une bonne table Silver:**
- ✅ Toutes les clés étrangères sont explicites et documentées
- ✅ Les noms de colonnes sont auto-explicatifs
- ✅ Les unités sont indiquées dans les noms de colonnes (`_m2`, `_euros`)
- ✅ Les métadonnées de traçabilité sont présentes
- ✅ Pas de colonnes avec des noms ambigus nécessitant de consulter la documentation
- ✅ Pas de codes ou valeurs magiques non documentées

**❌ Table avec dépendances cachées:**
```sql
CREATE TABLE fact_loyer_commune (
    id                          INTEGER,        -- ❌ Nom non explicite
    zone_id                     STRING,         -- ❌ Quelle dimension?
    value                       DECIMAL,        -- ❌ Valeur de quoi? Quelle unité?
    lower                       DECIMAL,        -- ❌ Borne de quoi?
    upper                       DECIMAL,        -- ❌ Idem
    type                        INTEGER,        -- ❌ Code non explicite
    -- ❌ Manque: métadonnées, liens clairs, unités
);
```

**Application lors de la conception:**

1. **Nommage explicite:** Utilisez `NAMING_CONVENTIONS.md`
2. **Documentation:** Ajoutez des `COMMENT ON COLUMN` pour chaque colonne
3. **Contraintes:** Définissez toutes les FK, PK, CHECK constraints
4. **Tests:** Validez l'intégrité avec des tests dbt
5. **Auto-description:** La table doit se comprendre sans documentation externe

**Validation:**
- [ ] Un analyste peut-il comprendre la table sans demander d'aide?
- [ ] Les jointures nécessaires sont-elles évidentes?
- [ ] Les unités de mesure sont-elles claires?
- [ ] La provenance et la fraîcheur des données sont-elles traçables?

---

### 📐 Résumé des Principes

| # | Principe | Action |
|---|----------|--------|
| 1 | Une table = une entité | Identifiez clairement le concept métier |
| 2 | Une clé = une identité | Utilisez des clés de substitution simples |
| 3 | Colonnes décrivent la clé | Pas de dépendances entre colonnes non-clé |
| 4 | Pas de dépendances transitives | Normalisez en 3NF minimum |
| 5 | Pas de duplication | Centralisez les référentiels stables |
| 6 | Tables auto-suffisantes | Colonnes explicites et documentées |

---

## ✅ ACTIONS OBLIGATOIRES - TOUTES LES TABLES

### 1. Ajout des Colonnes de Métadonnées

> **PRIORITÉ CRITIQUE**

**Action:** Ajouter les 4 colonnes de métadonnées obligatoires à **TOUTES** les tables (dimensions et faits) :

```sql
-- À ajouter sur chaque table
job_insert_id VARCHAR(255) NOT NULL,
job_insert_date_utc TIMESTAMP NOT NULL,
job_modify_id VARCHAR(255) NOT NULL,
job_modify_date_utc TIMESTAMP NOT NULL
```

**Implémentation dbt:**
- Ajouter ces champs dans tous les modèles Silver
- Populer `job_insert_id` avec le nom du modèle dbt (ex: `'dbt_silver_dim_commune'`)
- Utiliser `CURRENT_TIMESTAMP` pour les dates

---

### 2. Normalisation des Noms de Colonnes

**Action:** Renommer toutes les colonnes pour respecter les conventions de nommage :

✅ **RÈGLES DE BASE:**
- Passer tous les noms de colonnes en **lowercase** (minuscules)
- Utiliser **underscore** `_` comme séparateur
- **Français** pour les entités du domaine uniquement (loyer, commune, logement, proprietaire)
- **Anglais** pour tout le reste (predicted, observed, lower, upper, count, et tout le technique)
- **Pas d'abréviations** sauf exceptions autorisées (m2, r2, epci, insee, utc, count/sum/avg)

✅ **SUFFIXES TECHNIQUES (anglais):**
- `_sk` pour les clés de substitution (Surrogate Key)
- `_code` pour les codes officiels
- `_label` pour les libellés officiels
- `_date_utc` pour les champs de type date/timestamp en UTC
- `_id` pour les identifiants système

**Exemples de transformation:**
```sql
-- ❌ AVANT (mauvaises conventions)
"Ville", "Code_postal", "Latitude", "UPD_DATE", "nb_obs"

-- ✅ APRÈS (nouvelles conventions)
ville, code_postal, latitude, modify_date_utc, count_observations
```

**Exemples entités domaine vs opérations:**
```sql
-- Entités du domaine → Français
loyer_m2_moy, commune_label, row_sk, proprietaire_sk

-- Opérations/attributs → Anglais
predicted, observed, lower_bound, upper_bound, count_observations

-- Technique (toujours anglais)
commune_sk, job_insert_date_utc, job_modify_id
```

---

### 3. Clés de Substitution (Surrogate Keys)

**Action:** Implémenter des clés de substitution sur toutes les dimensions et faits :

**Méthode 1 (recommandée):** Hash dbt
```sql
{{ dbt_utils.generate_surrogate_key(['code_commune']) }} AS commune_sk
```

**Méthode 2 (alternative):** Serial Integer pour performances
```sql
commune_sk SERIAL PRIMARY KEY
```

**À appliquer sur:**
- Toutes les tables de dimensions (préfixe `dim_`)
- Toutes les tables de faits (préfixe `fact_`)

**Format des clés:**
- Dimensions: `<nom_dimension>_sk` (ex: `commune_sk`, `gare_sk`)
- Faits: `<nom_fait>_sk` (ex: `row_sk`, `zone_attraction_sk`)

---

### 4. Codes Officiels

**Action:** Stocker les codes officiels normalisés avec le suffixe `_code` :

```sql
commune_insee_code VARCHAR(5) NOT NULL  -- Code INSEE normalisé
epci_code VARCHAR(9) NOT NULL           -- Code EPCI officiel
```

**Règle:** Les `_code` contiennent toujours les codes officiels normalisés (INSEE, SIRET, etc.).

---

## 🗂️ ACTIONS PAR TABLE

### TABLE: `geo` → `DIM_COMMUNE`

**Instructions:**

1. [ ] Renommer la table: `geo` → `dim_commune`
2. [ ] Ajouter `commune_sk` (clé de substitution)
3. [ ] Renommer `CODGEO` → `commune_insee_code` (code officiel)
4. [ ] Renommer `LIBGEO` → `commune_label` (libellé officiel)
5. [ ] Extraire `departement_code` depuis les 2 premiers caractères du code commune
7. [ ] Ajouter `region_code` (à mapper depuis table de référence)
8. [ ] Ajouter les 4 colonnes de métadonnées obligatoires (en anglais: job_*)
9. [ ] Créer contrainte UNIQUE sur `commune_insee_code`
10. [ ] Créer index sur `departement_code` et `region_code`
11. [ ] Ajouter commentaires SQL en français sur la table et les colonnes

> **Note convention**: `commune_insee_code` et `commune_label` utilisent des termes français pour l'entité (commune) car c'est un concept métier, mais `_insee_code` et `_label` sont des suffixes techniques standardisés.

---

### TABLE: `accueillants` → `DIM_ACCUEILLANT`

**Instructions:**

1. [ ] Renommer la table: `accueillants` → `dim_accueillant`
2. [ ] Ajouter `accueillant_sk` (clé de substitution)
3. [ ] Passer toutes les colonnes en lowercase
5. [ ] Normaliser le champ `code_postal` (supprimer espaces)
6. [ ] **Enrichissement géographique:** Ajouter `commune_sk` via jointure avec `dim_commune` sur code postal
7. [ ] Ajouter contrainte CHECK sur latitude (-90 à 90) et longitude (-180 à 180)
8. [ ] Ajouter foreign key: `commune_sk` → `dim_commune(commune_sk)`
9. [ ] Ajouter les 4 colonnes de métadonnées obligatoires
10. [ ] Standardiser les valeurs du champ `statut` (valeurs contrôlées)
11. [ ] Créer index sur `commune_sk`, `statut`, `code_postal`
12. [ ] Ajouter commentaires SQL en français

---

### TABLE: `gares` → `DIM_GARE`

**Instructions:**

1. [ ] Renommer la table: `gares` → `dim_gare`
2. [ ] Ajouter `gare_sk` (clé de substitution)
3. [ ] Passer toutes les colonnes en lowercase
5. [ ] Convertir `fret` et `voyageurs` de O/N vers BOOLEAN
6. [ ] Renommer `departemen` → `departement`
7. [ ] **Enrichissement géographique:** Ajouter `commune_sk` via jointure avec `dim_commune`
8. [ ] Ajouter foreign key: `commune_sk` → `dim_commune(commune_sk)`
9. [ ] Ajouter contrainte CHECK: au moins un service (fret OR voyageurs) doit être actif
10. [ ] Conserver `ingestion_timestamp` depuis Bronze
11. [ ] Ajouter les 4 colonnes de métadonnées obligatoires
12. [ ] Créer index sur `commune_sk`, `code_ligne`, `departement`
13. [ ] Ajouter commentaires SQL en français

---

### TABLE: `logement` → `FACT_LOYER_ANNONCE`

**Instructions:**

1. [ ] Renommer la table: `logement` → `fact_loyer_annonce`
2. [ ] Ajouter `row_sk` (clé de substitution)
3. [ ] Passer toutes les colonnes en lowercase
4. [ ] Ajouter `commune_sk` via jointure avec `dim_commune` sur `code_commune`
5. [ ] Renommer les colonnes selon conventions (français pour métier):
   - `lib_commune` → supprimer (redondant avec dimension)
   - `lib_epci` → supprimer (obtenir via FK)
   - `lib_dep` → supprimer (obtenir via dimension)
   - `lib_reg` → supprimer (obtenir via dimension)
6. [ ] Convertir `prix_loyer`, `borne_inf_pred`, `borne_sup_pred` de STRING vers DECIMAL(10,2)
7. [ ] **Renommer selon nouvelles conventions** (entités françaises, attributs anglais):
   - `prix_loyer` → `loyer_predicted_m2` (entité: loyer, attribut: predicted, unité: m2)
   - `borne_inf_pred` → `loyer_lower_bound_m2`
   - `borne_sup_pred` → `loyer_upper_bound_m2`
   - `niveau_pred` → `prediction_level`
   - `rescued_data` → `data_rescued`
   - `code_epci` → `epci_code` (conserver comme attribut)
8. [ ] Ajouter contrainte CHECK: `loyer_predicted_m2 > 0`
9. [ ] Ajouter contrainte CHECK: `loyer_lower_bound_m2 < loyer_upper_bound_m2`
10. [ ] Ajouter foreign key: `commune_sk` → `dim_commune(commune_sk)`
11. [ ] Ajouter les 4 colonnes de métadonnées obligatoires
12. [ ] Créer index sur `commune_sk`, `code_departement`, `code_region`
13. [ ] Ajouter commentaires SQL en français

---

### TABLE: `zones_attraction` → `FACT_ZONE_ATTRACTION`

**Instructions:**

1. [ ] Renommer la table: `zones_attraction` → `fact_zone_attraction`
2. [ ] Ajouter `zone_attraction_sk` (clé de substitution)
3. [ ] Passer toutes les colonnes en lowercase
4. [ ] Renommer les colonnes (termes français pour métier):
   - `CODGEO` → Utiliser pour créer `commune_sk` (pas stocker le code brut)
   - `LIBGEO` → supprimer (obtenir via dimension)
   - `CODEAAV` → `aire_attraction_code`
   - `CODGEOAAV` → Utiliser pour créer `commune_pole_sk`
   - `LIBAAV2020` → `aire_attraction_label`
   - `CATEAAV` → `aire_attraction_categorie`
   - `DEP` → `departement_code`
   - `REG` → `region_code`
5. [ ] **Enrichissements géographiques:**
   - Ajouter `commune_sk` via jointure sur code INSEE (CODGEO)
   - Ajouter `commune_pole_sk` via jointure sur code INSEE pôle (CODGEOAAV)
6. [ ] Ajouter foreign keys:
   - `commune_sk` → `dim_commune(commune_sk)`
   - `commune_pole_sk` → `dim_commune(commune_sk)`
7. [ ] Ajouter les 4 colonnes de métadonnées obligatoires (anglais: job_*)
8. [ ] Créer index sur `commune_sk`, `commune_pole_sk`, `aire_attraction_code`, `aire_attraction_categorie`
9. [ ] Ajouter commentaires SQL en français

> **Note**: "aire_attraction" est en français car c'est un concept métier du zonage INSEE

---

### TABLE: `siae_structures` → `DIM_SIAE_STRUCTURE`

**Instructions:**

1. [ ] Renommer la table: `siae_structures` → `dim_siae_structure`
2. [ ] Ajouter `siae_structure_sk` (clé de substitution)
3. [ ] Passer toutes les colonnes en lowercase
5. [ ] Renommer les colonnes:
   - `structure_type` → `type_structure`
   - `legal_name` → `raison_sociale`
   - `trade_name` → `enseigne`
   - `phone` → `telephone`
   - `website` → `site_web`
   - `accepting_applications` → `accepte_candidatures` (BOOLEAN)
   - `address_line_1` → `adresse_ligne1`
   - `address_line_2` → `adresse_ligne2`
   - `postal_code` → `code_postal`
   - `city` → `ville`
   - `department` → `departement`
   - `insee_code` → utiliser pour jointure
   - `standardized_city_name` → `ville_standardisee`
   - `created_at` → `date_creation`
   - `updated_at` → `date_mise_a_jour`
6. [ ] **Enrichissement géographique:** Ajouter `commune_sk` via jointure avec `dim_commune` sur `insee_code`
7. [ ] Valider le format SIRET (14 chiffres): `CHECK (siret ~ '^[0-9]{14}$')`
8. [ ] Valider `structure_type`: `CHECK (structure_type IN ('ETTI', 'ACI', 'EI', 'AI'))`
9. [ ] Ajouter foreign key: `commune_sk` → `dim_commune(commune_sk)`
10. [ ] Ajouter contrainte UNIQUE sur `siret`
11. [ ] Ajouter les 4 colonnes de métadonnées obligatoires (anglais: job_*)
12. [ ] Créer index sur `commune_sk`, `structure_type`, `departement`, `siret`
13. [ ] Ajouter commentaires SQL en français

> **Note**: Conserver `structure_type` plutôt que `type_structure` car "type" est un attribut descriptif de "structure"

---

### TABLE: `siae_postes` → `FACT_SIAE_POSTE`

**Instructions:**

1. [ ] Renommer la table: `siae_postes` → `fact_siae_poste`
2. [ ] Ajouter `siae_poste_sk` (clé de substitution)
3. [ ] Passer toutes les colonnes en lowercase
5. [ ] Ajouter `siae_structure_sk` via jointure avec `dim_siae_structure`
6. [ ] Créer les colonnes (à extraire depuis les données Bronze):
   - `contrat_type` (type de contrat)
   - `rome_code` (code ROME)
   - `rome_label` (libellé métier ROME)
   - `poste_disponible` (BOOLEAN)
   - `postes_nombre` (INTEGER)
   - `poste_description` (TEXT)
   - `creation_date_utc` (date création)
   - `modification_date_utc` (date mise à jour)
   - `expiration_date_utc` (date expiration)
7. [ ] Valider le format code ROME: `CHECK (rome_code ~ '^[A-Z][0-9]{4}$')`
8. [ ] Valider `postes_nombre`: `CHECK (postes_nombre > 0)`
9. [ ] Ajouter foreign key: `siae_structure_sk` → `dim_siae_structure(siae_structure_sk)`
10. [ ] Ajouter les 4 colonnes de métadonnées obligatoires (anglais: job_*)
11. [ ] Créer index sur `siae_structure_sk`, `rome_code`, `poste_disponible`, `expiration_date_utc`
12. [ ] Ajouter commentaires SQL en français

> **Note**: Les suffixes `_code` et `_label` sont techniques (anglais), mais "rome" est conservé car c'est un acronyme officiel

---

### TABLE: `lignes` → `DIM_LIGNE`

**Instructions:**

1. [ ] Analyser la structure de `bronze.lignes` (schéma à définir)
2. [ ] Créer `dim_ligne` avec:
   - `ligne_sk` (clé de substitution)
   - `ligne_code` (code officiel)
   - `libelle` (nom de la ligne)
   - `categorie` (TGV/classique)
   - Point kilométrique début/fin
   - Tracé géographique si disponible
3. [ ] Ajouter les 4 colonnes de métadonnées obligatoires
4. [ ] Ajouter commentaires SQL en français

---

## 🔗 ENRICHISSEMENT GÉOGRAPHIQUE

### Principe Central

**Toutes les tables doivent être enrichies avec `commune_sk`** pour permettre les analyses à tous les niveaux (commune, département, région).

### Hiérarchie Géographique

```
RÉGION (code 2 chiffres)
  └── DÉPARTEMENT (code 2-3 chiffres)
      └── COMMUNE (code INSEE 5 chiffres)
```

### Méthode d'Enrichissement

**Dans les modèles dbt Silver:**

```sql
LEFT JOIN {{ ref('dim_commune') }} AS commune
    ON source.code_postal = commune.code_postal
    -- OU
    ON source.insee_code = commune.commune_insee_code
    -- OU
    ON UPPER(TRIM(source.ville)) = UPPER(commune.commune_label)
```

**Ajouter systématiquement:**
```sql
commune.commune_sk
```

---

## ⚠️ CAS PARTICULIERS

### Arrondissements Paris, Lyon, Marseille

> **ATTENTION:** Codes INSEE spéciaux

**Problème:** Les arrondissements ont des codes INSEE distincts (ex: `75101` pour Paris 01) qui doivent être remappés vers la commune parent (`75056` pour Paris).

**Solution:**

1. [ ] Créer une table de mapping `dim_arrondissement`:
   ```sql
   CREATE TABLE dim_arrondissement (
       arrondissement_code VARCHAR(5),
       commune_insee_code VARCHAR(5),
       arrondissement_label VARCHAR(255),
       FOREIGN KEY (commune_insee_code) REFERENCES dim_commune(commune_insee_code)
   );
   ```

2. [ ] Utiliser cette table lors de l'enrichissement géographique

---

## 📊 CONTRAINTES D'INTÉGRITÉ

### À Implémenter Systématiquement

**Primary Keys (PK):**
```sql
CONSTRAINT pk_<table> PRIMARY KEY (<table>_sk)
```

**Foreign Keys (FK):**
```sql
CONSTRAINT fk_<table>_<ref_table> 
    FOREIGN KEY (<ref_table>_sk) 
    REFERENCES <ref_table>(<ref_table>_sk)
```

**Unique Constraints:**
```sql
CONSTRAINT uq_<table>_<column> UNIQUE (<column>)
```

**Check Constraints:**
- Valeurs positives pour les montants
- Formats de codes (SIRET, ROME, etc.)
- Cohérence des bornes (min < max)
- Valeurs contrôlées (énumérations)

---

## 📝 DOCUMENTATION

### Commentaires SQL Obligatoires

**Sur chaque table:**
```sql
COMMENT ON TABLE <table> IS 'Description détaillée en français...';
```

**Sur chaque colonne:**
```sql
COMMENT ON COLUMN <table>.<column> IS 'Description en français...';
```

### Langue: Français

Toute la documentation doit être en **français**, y compris:
- Commentaires SQL
- Documentation dbt (fichiers `.yml`)
- Descriptions dans les métadonnées

---

## 🧪 TESTS DE QUALITÉ

### Tests dbt à Implémenter

**Pour chaque modèle Silver:**

```yaml
# schema.yml
models:
  - name: dim_commune
    description: "Référentiel géographique des communes françaises (MDM)"
    columns:
      - name: commune_sk
        description: "Clé de substitution (Surrogate Key) générée par hash dbt"
        tests:
          - unique
          - not_null
      
      - name: commune_insee_code
        description: "Code INSEE officiel de la commune (5 caractères)"
        tests:
          - unique
          - not_null
          - relationships:
              to: source('bronze', 'geo')
              field: CODGEO
```

**Tests obligatoires:**
- `unique` sur toutes les clés (SK et BK)
- `not_null` sur les colonnes obligatoires et métadonnées (job_*)
- `relationships` pour valider les FK
- `accepted_values` pour les énumérations (structure_type, niveau_prediction, etc.)
- Tests custom pour formats (SIRET, code postal, ROME, etc.)
- Tests de cohérence métier (bornes, dates, valeurs positives)

---

## 🚀 PLAN D'EXÉCUTION

### Phase 1: Tables Fondation (Semaine 1)
1. [ ] `DIM_COMMUNE` (priorité absolue - dépendance de toutes les autres)
2. [ ] `DIM_LIGNE`

### Phase 2: Dimensions Métier (Semaine 2)
3. [ ] `DIM_ACCUEILLANT`
4. [ ] `DIM_GARE`
5. [ ] `DIM_SIAE_STRUCTURE`

### Phase 3: Tables de Faits (Semaine 3)
6. [ ] `FACT_LOGEMENT`
7. [ ] `FACT_ZONE_ATTRACTION`
8. [ ] `FACT_SIAE_POSTE`

### Phase 4: Validation et Tests (Semaine 4)
9. [ ] Ajout de tous les tests dbt
10. [ ] Validation des contraintes d'intégrité
11. [ ] Documentation complète
12. [ ] Revue de code

---

## 📚 RÉFÉRENCES

- **Spécifications détaillées:** `modelisation_draft.md`
- **Architecture Médaillon:** Bronze → Silver → Gold
- **Convention de nommage:** lowercase, suffixes `_sk`, `_code`, `_label`, `_utc`
- **MDM Géographique:** Hiérarchie région/département/commune basée sur codes INSEE

---

## ❓ QUESTIONS / SUPPORT

Pour toute question ou clarification sur ces instructions:
1. Consulter le document `modelisation_draft.md` (section correspondante)
2. Vérifier les exemples de code DDL fournis
3. Contacter le Product Owner pour validation métier

---

**Bon courage! 🎯**
