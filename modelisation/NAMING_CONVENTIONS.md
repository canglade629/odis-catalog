# Conventions de Nommage – Data Modeling

> **Version**: 1.0  
> **Date**: 2025-12-01  
> **Contexte**: Architecture Medallion (Silver/Gold layers) – PostgreSQL

---

## Principes Généraux

### Règle Fondamentale : Séparation Métier / Technique

- **Concepts métier** → **Français** (entités, attributs, unités - tout ce qui a un sens business)
- **Concepts techniques** → **Anglais** (suffixes de clés, opérations, agrégations, métadonnées, stockage)

### Structure de Nommage

La structure générale suit ce pattern, avec application des **règles de concision** :

```
[indicateur_metier]_[qualificatif_abrege]_[suffixe_technique]
```

**Principes** :
- L'**indicateur métier** peut inclure l'unité quand elle fait partie intégrante du concept (ex: `loyer_m2`)
- Les **qualificatifs statistiques** (min, max, moy, nb...) sont abrégés et placés **à la fin**
- Les **concepts métier** ne sont **jamais abrégés**
- Les **suffixes techniques** (_sk, _code, _name...) sont toujours en anglais

**Exemples avec règles de concision appliquées** :
- `loyer_m2` → indicateur métier (loyer au mètre carré est un concept à part entière)
- `loyer_m2_min` → indicateur métier + qualificatif statistique (borne inférieure)
- `loyer_m2_max` → indicateur métier + qualificatif statistique (borne supérieure)
- `nb_observations_maille` → qualificatif abrégé + concept métier + contexte
- `commune_sk` → entité métier + suffixe technique

---

## Langue par Type de Concept

### 🇫🇷 Français (Concepts Métier)

**Entités du domaine** :
- `loyer`, `commune`, `epci`, `departement`, `region`, `pays`
- `annee`, `trimestre`, `mois`
- `logement`, `batiment`, `proprietaire`, `locataire`, `bailleur`

**Attributs métier** :
- `predit`, `observe`, `estime`, `calcule`
- `inferieur`, `superieur`, `moyen`, `median`
- `minimum`, `maximum`, `total`

**Concepts statistiques métier** :
- `maille` (zone statistique)
- `qualite` (qualité de prédiction)
- `borne` (intervalle de confiance)
- `nombre` (quantité)

> **Principe** : Si le concept a un sens métier ou business, utilisez le français.

### 🇬🇧 Anglais (Concepts Techniques Uniquement)

**Opérations et agrégations** :
- `count`, `sum`, `avg`, `min`, `max`
- Ces termes sont des opérateurs SQL/techniques standards

**Suffixes de clés** :
- `_sk` (Surrogate Key - clé de substitution)
- `_bk` (Business Key - clé métier technique interne au système source)
- `_code` (Code officiel avec signification universelle - ISO, INSEE)
- `_name` (Libellé ou nom)
- `_id` (Identifiant technique)
- `_date_utc` (Champs date/timestamp en UTC)

**Métadonnées système** :
- `job_insert_id`, `job_insert_date_utc`
- `job_update_id`, `job_update_date_utc`

---

## Règles d'Écriture

### 1. Format des Noms

- ✅ **Tout en minuscules** : `loyer_predit_m2`
- ✅ **Séparateur** : underscore `_`
- ❌ Pas de CamelCase : ~~`loyerPredit`~~
- ❌ Pas de majuscules : ~~`LOYER_PREDIT`~~

### 2. Abréviations

**Interdites (sauf exceptions)** :
- ❌ `typpred` → ✅ `type_prediction`
- ❌ `lwr` → ✅ `min` (abréviation statistique autorisée)
- ❌ `upr` → ✅ `max` (abréviation statistique autorisée)
- ❌ `nbobs` → ✅ `nb_observations` (abréviation `nb` autorisée)
- ❌ `nbr` → ✅ `nb` (abréviation statistique autorisée)
- ❌ `upd_date` → ✅ `update_date` ou `modify_date`
- ❌ Abréviations métier : `loy`, `surf`, `com`, `log`, `prop`

**Abréviations AUTORISÉES (statistiques/techniques uniquement)** :
- ✅ `min` / `max` : bornes inférieure/supérieure
- ✅ `moy` ou `avg` : moyenne
- ✅ `nb` : nombre
- ✅ `pct` : pourcentage
- ✅ `med` : médiane
- ✅ `std` : écart-type
- ✅ `m2` : mètre carré
- ✅ `r2` : coefficient de détermination
- ✅ Acronymes officiels : `epci`, `insee`, `siret`, `siren`
- ✅ `utc` : fuseau horaire
- ✅ Opérations SQL : `count`, `sum`, `avg`, `min`, `max` (comme préfixes)

### 3. Unités de Mesure

Toujours en suffixe, collées à l'attribut :
- `loyer_m2` (loyer au mètre carré)
- `loyer_m2_min`, `loyer_m2_max`, `loyer_m2_moy` (avec modificateurs statistiques)
- `surface_totale_m2` (surface totale en mètres carrés)
- `prix_euros_moy` (prix moyen en euros - modificateur à la fin)

### 4. Préfixes et Suffixes Obligatoires

**Suffixes techniques (toujours en anglais)** :
- `_sk` : Surrogate Key (clé de substitution) - utilisé pour les PK et FK
- `_bk` : Business Key (clé métier interne au système source, sans signification universelle)
- `_code` : Code officiel avec signification universelle (ex: code INSEE, ISO, SIRET)
- `_name` : Libellé ou nom (remplace `_label`)
- `_date_utc` : Champs de type date/timestamp en UTC
- `_id` : Identifiant technique système

**Préfixes de table** :
- `dim_` : Tables de dimension
- `fact_` : Tables de faits

---

## Règles de Concision

### Objectif : Noms Courts et Efficaces

Les noms de colonnes doivent être **explicites** mais **concis**. Éviter les noms trop longs qui nuisent à la lisibilité sans apporter de valeur.

### 1. Abréger les Attributs Statistiques/Techniques Courants

Pour les termes très fréquents et universellement compris en analyse de données, **utiliser des abréviations** :

| Concept Complet | Abréviation | Exemple d'Usage |
|-----------------|-------------|-----------------|
| `minimum` / `borne_inferieure` | `min` | `loyer_m2_min` |
| `maximum` / `borne_superieure` | `max` | `loyer_m2_max` |
| `moyenne` | `moy` ou `avg` | `loyer_m2_moy` |
| `nombre` | `nb` | `nb_observations_maille` |
| `pourcentage` | `pct` | `pct_variation` |
| `mediane` | `med` | `loyer_m2_med` |
| `ecart_type` | `std` | `loyer_m2_std` |

**Exemples de transformation :**
```
❌ loyer_borne_inferieure_m2  →  ✅ loyer_m2_min
❌ loyer_borne_superieure_m2  →  ✅ loyer_m2_max
❌ nombre_observations_maille  →  ✅ nb_observations_maille
```

### 2. Supprimer les Contextes Redondants

Si une information s'applique à **toute la table**, elle n'a pas sa place dans les noms de colonnes :

- ✅ Dans une table de prédictions → **pas de suffixe `_predit`**
- ✅ Dans une table historique → **pas de suffixe `_historique`**
- ✅ Dans une table agrégée → **pas de suffixe `_agrege`**

**Exemples de simplification :**
```
Table: fact_loyer_predit_commune
❌ loyer_predit_m2            →  ✅ loyer_m2
❌ loyer_predit_min_m2        →  ✅ loyer_m2_min
❌ loyer_predit_max_m2        →  ✅ loyer_m2_max

Table: fact_emploi_historique
❌ salaire_historique_moyen   →  ✅ salaire_moy
```

> **Principe** : Si l'information est dans le **nom de la table**, ne la répétez pas dans chaque colonne.

### 3. Ordre des Qualificatifs : Concept Métier d'Abord

Mettre les **modificateurs statistiques à la fin** pour regrouper visuellement les colonnes liées :

✅ **Correct (groupement logique)** :
```sql
loyer_m2          -- Mesure principale
loyer_m2_min      -- Borne inférieure
loyer_m2_max      -- Borne supérieure
loyer_m2_moy      -- Moyenne
loyer_m2_med      -- Médiane
loyer_m2_std      -- Écart-type
```

❌ **Incorrect (colonnes dispersées)** :
```sql
min_loyer_m2
loyer_m2
loyer_moy_m2
max_loyer_m2
loyer_median_m2
```

**Avantages :**
- Les colonnes se retrouvent **côte à côte** en tri alphabétique
- Identification rapide du **concept métier principal** (`loyer_m2`)
- Meilleure lisibilité dans les requêtes SQL

### 4. Exception : Ne Jamais Abréger les Concepts Métier

Les termes du **domaine métier** doivent rester **explicites** :

✅ **Toujours écrire en entier** :
- `loyer`, `surface`, `commune`, `logement`
- `proprietaire`, `locataire`, `bailleur`
- `qualite`, `prediction`, `observation`

❌ **Jamais abréger** :
- ~~`loy`~~, ~~`surf`~~, ~~`com`~~, ~~`log`~~
- ~~`prop`~~, ~~`loc`~~, ~~`bail`~~
- ~~`qual`~~, ~~`pred`~~, ~~`obs`~~

---

### ✅ Règles de Concision - Résumé

| # | Règle | Action |
|---|-------|--------|
| 1 | Abréger les opérations/qualificatifs techniques | `min`, `max`, `nb`, `moy`, `pct`, `med`, `std` |
| 2 | Supprimer les contextes applicables à toute la table | Pas de `_predit` si table = prédictions |
| 3 | Concept métier d'abord, qualificatif à la fin | `loyer_m2_min` pas `min_loyer_m2` |
| 4 | Ne jamais abréger les concepts métier | `loyer` pas `loy` |

---

### 📋 Exemples Complets de Simplification

```sql
-- ❌ AVANT (noms trop longs et redondants)
CREATE TABLE fact_loyer_predit_commune (
    loyer_predit_m2                     DECIMAL(10,2),
    loyer_borne_inferieure_predit_m2    DECIMAL(10,2),
    loyer_borne_superieure_predit_m2    DECIMAL(10,2),
    nombre_observations_maille          INTEGER,
    qualite_prediction_r2_ajuste        DECIMAL(5,4)
);

-- ✅ APRÈS (noms concis et efficaces)
CREATE TABLE fact_loyer_predit_commune (
    loyer_m2                            DECIMAL(10,2),
    loyer_m2_min                        DECIMAL(10,2),
    loyer_m2_max                        DECIMAL(10,2),
    nb_observations_maille              INTEGER,
    qualite_r2_ajuste                   DECIMAL(5,4)
);
```

> **Note** : `qualite_r2_ajuste` conserve "qualite" car c'est un concept métier dans le contexte de la modélisation prédictive.

---

## Types de Colonnes

### Clés (Keys)

```sql
-- Clé de substitution (hash dbt ou serial)
loyer_commune_sk        STRING    -- PK

-- Clés métier et codes
commune_bk              STRING    -- Clé interne système source
commune_code            STRING    -- Code INSEE (universel)
commune_name            STRING    -- Nom de la commune

-- Clés étrangères (utilisent _sk)
commune_sk              STRING    -- FK to dim_commune
epci_sk                 STRING    -- FK to dim_epci
```

### Mesures (Facts)

```sql
-- Mesures principales (français)
loyer_m2                       DECIMAL(10,2)   -- Contexte "prédit" dans le nom de table
loyer_observe_m2               DECIMAL(10,2)

-- Bornes d'intervalle (abréviations statistiques)
loyer_m2_min                   DECIMAL(10,2)
loyer_m2_max                   DECIMAL(10,2)
loyer_m2_moy                   DECIMAL(10,2)
loyer_m2_med                   DECIMAL(10,2)

-- Compteurs (abréviation nb)
nb_observations_maille         INTEGER
nb_observations_commune        INTEGER
nb_annonces                    INTEGER
```

### Indicateurs de Qualité

```sql
-- Métriques statistiques (français + abréviations)
qualite_r2_ajuste              DECIMAL(5,4)    -- Entre 0 et 1
score_confiance_pct            DECIMAL(5,4)    -- Pourcentage de confiance
taux_completude_pct            DECIMAL(5,4)    -- Pourcentage de complétude
```

### Métadonnées Obligatoires

**Chaque table doit contenir** :

```sql
job_insert_id           STRING          -- Job ayant inséré la ligne
job_insert_date_utc     TIMESTAMP_NTZ   -- Date d'insertion (UTC)
job_update_id           STRING          -- Job ayant modifié la ligne
job_update_date_utc     TIMESTAMP_NTZ   -- Date de modification (UTC)
```

### Colonne Extras (Pattern Core/Extras)

**Principe** : Éviter la sur-normalisation en utilisant un stockage hybride.

**Colonne obligatoire** :
```sql
extras                  VARIANT         -- Champs semi-structurés (JSON)
```

**Rule of Thumb** :
- **Colonnes core** : colonnes stables + clés + attributs des clés (colonnes structurées classiques)
- **Colonne extras** : VARIANT pour le reste (champs peu utilisés, instables, ou dont l'usage n'est pas encore confirmé)

**Quand utiliser `extras` ?**
- Champs dont l'utilité analytique n'est pas encore prouvée
- Attributs variant selon la source de données
- Metadata complémentaire ne justifiant pas une colonne dédiée
- Champs expérimentaux ou en phase de test

**Pattern de promotion** :
Quand un champ stocké dans `extras` devient fréquemment utilisé dans les requêtes analytiques :
→ Promouvoir en colonne core dédiée OU créer une table normalisée séparée avec FK

**Exemple** :
```sql
-- Requête détectant un usage fréquent
SELECT 
    COUNT(*) as nb_requetes
FROM query_history 
WHERE query_text LIKE '%extras.nom_attribut%'
  AND execution_date >= CURRENT_DATE - 30;

-- Si nb_requetes > seuil → Promotion
ALTER TABLE ma_table ADD COLUMN nom_attribut VARCHAR(255);
UPDATE ma_table SET nom_attribut = extras:nom_attribut;
```

**Avantages** :
- ✅ Flexibilité : ajout de nouveaux attributs sans migration de schéma
- ✅ Évite la prolifération de colonnes peu utilisées
- ✅ Permet l'évolution progressive du modèle de données
- ✅ Réduit le coût de stockage (colonnes non indexées par défaut)

**Inconvénients** :
- ⚠️ Performance : requêtes sur VARIANT moins performantes que colonnes natives
- ⚠️ Typage : pas de validation de type automatique (responsabilité développeur)

---

## Entités MDM (Master Data Management)

Les entités suivantes sont gérées par le MDM : `commune`, `departement`, `region`, `pays`.

### Structure Standard

```sql
CREATE TABLE dim_<entite> (
    <entite>_sk      STRING,          -- Clé de substitution (hash dbt)
    <entite>_bk      STRING,          -- Clé métier source (si applicable)
    <entite>_code    STRING,          -- Code officiel universel (INSEE, ISO)
    <entite>_name    STRING,          -- Libellé/nom officiel
    
    -- Métadonnées obligatoires
    job_insert_id        STRING,
    job_insert_date_utc  TIMESTAMP_NTZ,
    job_update_id        STRING,
    job_update_date_utc  TIMESTAMP_NTZ,
    
    -- Champs semi-structurés
    extras               VARIANT,
    
    PRIMARY KEY (<entite>_sk)
);
```

**Exemple concret** :

```sql
CREATE TABLE dim_commune (
    commune_sk           STRING,
    commune_bk           STRING,        -- Clé source si applicable
    commune_code         STRING,        -- Code INSEE (universel)
    commune_name         STRING,        -- Nom officiel
    
    job_insert_id        STRING,
    job_insert_date_utc  TIMESTAMP_NTZ,
    job_update_id        STRING,
    job_update_date_utc  TIMESTAMP_NTZ,
    
    extras               VARIANT,
    
    PRIMARY KEY (commune_sk)
);
```

### Codes Géographiques

- **Région** : Code INSEE région (2 chiffres)
- **Département** : Code INSEE département (2 ou 3 chiffres)
- **Commune** : Code INSEE commune (5 chiffres)
- **Arrondissement** : Certains codes (ex: `75101` pour Paris 01) réfèrent à la commune parent (`75056`)

---

## Arbre de Décision pour le Nommage

Lorsque vous nommez une nouvelle colonne, posez-vous ces questions :

### 1️⃣ Est-ce un concept métier spécifique à votre domaine ?
→ **Français**  
Exemples : `loyer`, `commune`, `maille`, `predit`

### 2️⃣ Est-ce un concept technique/système transversal ?
→ **Anglais**  
Exemples : `_sk`, `_id`, `job_*`, `_date_utc`

### 3️⃣ Est-ce un terme statistique/mathématique universel ?
→ **Conserver l'original**  
Exemples : `r2` (pas `r_deux`), `m2` (pas `metre_carre`)

### 4️⃣ L'abréviation causerait-elle de la confusion ?
→ **Écrire en entier**  
Exemples : `nombre_observations` pas `nb_obs`

---

## Exemples Complets

### Table de Faits : Loyers Communaux

```sql
CREATE TABLE fact_loyer_commune (
    -- Clé primaire
    loyer_commune_sk                STRING,
    
    -- Clés étrangères (dimensions) - toujours _sk
    commune_sk                      STRING,
    epci_sk                         STRING,
    annee_sk                        STRING,
    type_prediction_sk              STRING,
    
    -- Faits (mesures) - français avec abréviations statistiques
    loyer_m2                        DECIMAL(10,2),  -- Mesure principale
    loyer_m2_min                    DECIMAL(10,2),  -- Borne inférieure
    loyer_m2_max                    DECIMAL(10,2),  -- Borne supérieure
    loyer_m2_moy                    DECIMAL(10,2),  -- Moyenne
    
    -- Indicateurs de qualité - français
    qualite_r2_ajuste               DECIMAL(5,4),
    nb_observations_maille          INTEGER,
    nb_observations_commune         INTEGER,
    
    -- Métadonnées obligatoires
    job_insert_id                   STRING,
    job_insert_date_utc             TIMESTAMP_NTZ,
    job_update_id                   STRING,
    job_update_date_utc             TIMESTAMP_NTZ,
    
    -- Champs semi-structurés
    extras                          VARIANT,
    
    PRIMARY KEY (loyer_commune_sk),
    FOREIGN KEY (commune_sk) REFERENCES dim_commune(commune_sk),
    FOREIGN KEY (epci_sk) REFERENCES dim_epci(epci_sk),
    FOREIGN KEY (type_prediction_sk) REFERENCES dim_type_prediction(type_prediction_sk)
);

-- Commentaires (en français)
COMMENT ON TABLE fact_loyer_commune IS 
    'Table de faits contenant les loyers prédits par commune pour le 3ème trimestre 2018';

COMMENT ON COLUMN fact_loyer_commune.loyer_m2 IS 
    'Loyer prévu (charges comprises) au mètre carré, estimé à partir des annonces en ligne';

COMMENT ON COLUMN fact_loyer_commune.qualite_r2_ajuste IS 
    'Coefficient de détermination ajusté (R² ajusté) mesurant la qualité du modèle prédictif';
```

### Table de Dimension : Types de Prédiction

```sql
CREATE TABLE dim_type_prediction (
    type_prediction_sk      STRING,
    type_prediction_bk      STRING,
    type_prediction_code    STRING,
    type_prediction_name    STRING,
    
    job_insert_id           STRING,
    job_insert_date_utc     TIMESTAMP_NTZ,
    job_update_id           STRING,
    job_update_date_utc     TIMESTAMP_NTZ,
    
    extras                  VARIANT,
    
    PRIMARY KEY (type_prediction_sk)
);

COMMENT ON TABLE dim_type_prediction IS 
    'Type de maille statistique utilisée pour la prédiction du loyer (commune, EPCI, grille)';
```

---

## Checklist de Migration

Lors de la migration d'une table avec anciens noms :

- [ ] Créer un glossaire de mappage (ancien → nouveau)
- [ ] Documenter les termes français avec traductions anglaises dans les `COMMENT`
- [ ] Mettre à jour tous les modèles dbt référençant ces colonnes
- [ ] Ajouter les `COMMENT ON COLUMN` pour documentation
- [ ] Vérifier la cohérence avec les tables liées (FK)
- [ ] Créer les dimensions manquantes si nécessaire
- [ ] Valider avec les utilisateurs métier finaux

---

## Glossaire de Référence

| Concept | Français | Anglais | Dans les colonnes |
|---------|----------|---------|-------------------|
| **Entités du domaine** | | | |
| Loyer | loyer | rent | `loyer_predit_m2` ✅ |
| Commune | commune | municipality | `commune_sk`, `commune_code`, `commune_name` ✅ |
| EPCI | epci | intercommunality | `epci_sk`, `epci_code` ✅ |
| Logement | logement | housing | `logement_sk` ✅ |
| Propriétaire | proprietaire | owner | `proprietaire_sk` ✅ |
| **Attributs métier** | | | |
| Prédit | prédit | predicted | `predit` ✅ (français) |
| Observé | observé | observed | `observe` ✅ (français) |
| Inférieur | inférieur | lower | `inferieur` ✅ (français) |
| Supérieur | supérieur | upper | `superieur` ✅ (français) |
| Maille | maille | grid | `maille` ✅ (français) |
| Qualité | qualité | quality | `qualite` ✅ (français) |
| Nombre | nombre | count/number | `nombre` ✅ (français, pas `nbr`) |
| **Suffixes techniques** | | | |
| Clé substitution | - | surrogate key | `_sk` ✅ |
| Clé métier source | - | business key | `_bk` ✅ |
| Code universel | - | code | `_code` ✅ |
| Libellé/nom | - | name/label | `_name` ✅ |
| **Opérations** | | | |
| Compter | - | count | `count_` ✅ (opération SQL) |
| Sommer | - | sum | `sum_` ✅ (opération SQL) |
| Moyenne | - | average | `avg_` ✅ (opération SQL) |

---

## Références

- **Documentation interne** : `INSTRUCTIONS_MODELISATION.md`
- **Architecture** : Medallion (Bronze → Silver → Gold)
- **Base de données** : PostgreSQL
- **Outil de transformation** : dbt
- **Référentiels géographiques** : Codes INSEE (France)
