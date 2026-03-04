# Modélisation - Structure Data·Inclusion

**Source** : [Schéma data·inclusion - Structure](https://gip-inclusion.github.io/data-inclusion-schema/latest/structure/)  
**Date** : 2026-01-06

---

## Table : SIAE_STRUCTURE

Table principale consolidant les informations sur les structures d'inclusion (associations, centres sociaux, missions locales, etc.).

### Mapping Bronze → Silver

| Colonne cible | Transformation | Champ source | Description |
|---------------|----------------|--------------|-------------|
| **Clés et identifiants** ||||
| `siae_structure_sk` | `hash(id)` | `id` | Clé technique unique générée à partir de l'identifiant source. Utilisée pour les jointures et reste constante même si l'identifiant source évolue. |
| `siae_structure_bk` | Directe | `id` | Identifiant unique national : combinaison du code source et de l'identifiant local (exemple : "emplois-de-linclusion--17" ou "dora--AidantsConnect:2024-47BXY"). Garantit l'unicité de chaque structure à l'échelle nationale. |
| `siae_structure_siret_code` | Directe | `siret` | Numéro SIRET : identifiant officiel de l'établissement dans le répertoire SIRENE (14 chiffres). Permet de relier la structure aux données administratives françaises. Vérifié régulièrement par data·inclusion qui retire les SIRET invalides. |
| **Informations descriptives** ||||
| `siae_structure_label` | Directe | `nom` | Appellation officielle de l'établissement (exemple : "Centre social Le Tournesol", "Mission Locale de Paris"). Limité entre 3 et 150 caractères pour garantir la qualité des données. |
| `siae_structure_description` | Directe | `description` | Description textuelle libre détaillant les activités et la mission de l'établissement (exemple : "L'association offre un accès gratuit aux arts, à la culture et au sport pour toutes et tous sans distinction"). Aide à comprendre le type de services proposés. |
| **Localisation** ||||
| `commune_sk` | `hash(code_insee)` | `code_insee` | Clé technique pointant vers le référentiel des communes françaises (DIM_COMMUNE). Permet d'exploiter la hiérarchie géographique : commune → département → région. Attention : certains codes d'arrondissements (ex: 75101 pour Paris 1er) doivent être remontés à la commune parente. |
| `adresse` | Directe | `adresse` | Numéro et nom de voie de la structure. Essentiel pour localiser physiquement l'établissement et générer des itinéraires. |
| `complement_adresse` | Directe | `complement_adresse` | Informations additionnelles pour localiser précisément la structure (bâtiment, étage, porte, résidence, etc.). Améliore l'accessibilité physique du lieu. |
| `longitude` | Directe | `longitude` | Longitude GPS (WGS84) : coordonnée géographique Est-Ouest au format décimal. Permet l'affichage sur carte, le calcul de distances et les recherches "à proximité". |
| `latitude` | Directe | `latitude` | Latitude GPS (WGS84) : coordonnée géographique Nord-Sud au format décimal. Complète la longitude pour un positionnement géographique précis sur les cartes numériques. |
| **Contact** ||||
| `telephone` | Directe | `telephone` | Numéro de téléphone pour obtenir des renseignements sur la structure. Format recommandé E.164 avec indicatif international (exemple : "+33123456789"). Canal essentiel pour les publics éloignés du numérique. |
| `courriel` | Directe | `courriel` | Contact électronique officiel de la structure. Respecte le format RFC 5322. Permet les échanges dématérialisés et l'envoi de demandes de renseignements. |
| `site_web` | Directe | `site_web` | URL du site web officiel de la structure. Donne accès à des informations détaillées, actualités, formulaires de contact, et démarches en ligne proposées par la structure. |
| **Métadonnées obligatoires** ||||
| `job_insert_id` | Métadonnée | - | Identifiant du processus technique ayant inséré cette structure dans le data warehouse. Utile pour le support et l'audit des chargements de données. |
| `job_insert_date_utc` | Métadonnée | - | Horodatage de l'ajout de cette structure dans le système. Permet de suivre l'historique d'enrichissement du référentiel. |
| `job_update_id` | Métadonnée | - | Identifiant du dernier processus ayant modifié cette structure. Permet de retracer les mises à jour et identifier d'éventuelles anomalies. |
| `job_update_date_utc` | Métadonnée | - | Horodatage de la dernière mise à jour dans notre système. Permet de détecter les structures non synchronisées. |
| **Champs semi-structurés** ||||
| `extras` | Stockage VARIANT | Multiples | Stocke les champs source moins utilisés ou instables dans un format JSON. Permet d'éviter la prolifération de colonnes avant validation des besoins analytiques. |

### Contenu de la colonne `extras`

La colonne `extras` (type VARIANT/JSON) contient les champs suivants :

| Champ dans extras | Champ source | Description |
|-------------------|--------------|-------------|
| `source` | `source` | Code du producteur de données (ex: "emplois-de-linclusion", "france-travail", "dora"). Permet de tracer l'origine des données et de distinguer les différentes sources nationales. |
| `lien_source` | `lien_source` | URL permettant d'accéder à la page complète de la structure sur le site du producteur de données. Utile pour obtenir des informations complémentaires ou actualisées. |
| `code_postal` | `code_postal` | Code postal à 5 chiffres de l'adresse de la structure. Complète les informations de localisation et facilite les recherches géographiques approximatives. |
| `commune_label` | `commune` | Nom de la commune (redondant avec commune_sk mais utile pour affichage rapide). |
| `date_maj` | `date_maj` | Date de dernière mise à jour de la structure chez le producteur. Indicateur de fraîcheur : plus la date est récente, plus les informations sont probablement à jour. |
| `horaires_accueil` | `horaires_accueil` | Plages horaires d'accueil du public au format OpenStreetMap Opening Hours (exemple : "Mo-Fr 08:30-12:30; PH off"). Format standardisé permettant l'interprétation automatique par les applications. |
| `accessibilite_lieu` | `accessibilite_lieu` | Lien vers la fiche Accesslibre décrivant le niveau d'accessibilité du lieu pour les personnes à mobilité réduite (rampes, ascenseurs, toilettes adaptées, etc.). Information cruciale pour l'inclusion des publics en situation de handicap. |
| `reseaux_porteurs` | `reseaux_porteurs[]` | Tableau des codes de réseaux auxquels appartient la structure (exemples : "mission-locale", "ccas", "iae"). Une structure peut appartenir à plusieurs réseaux simultanément. |

---

## Notes d'implémentation

### Pattern de promotion

Quand un champ stocké dans `extras` devient fréquemment utilisé dans les requêtes analytiques :
→ Le promouvoir en colonne core dédiée OU créer une table normalisée séparée avec FK

**Exemple de détection** :
```sql
SELECT 
    COUNT(*) as nb_requetes
FROM query_history 
WHERE query_text LIKE '%extras:source%'
  AND execution_date >= CURRENT_DATE - 30;
```

**Candidats potentiels à la promotion** :
- `extras.source` → Si besoin de filtrer/grouper fréquemment par producteur
- `extras.reseaux_porteurs` → Si analyses intensives par type de réseau (nécessiterait explosion + table de fait)
- `extras.code_postal` → Si recherches géographiques fréquentes par code postal

### Gestion des codes INSEE

- Le champ `code_insee` de la source est utilisé pour faire le lien avec `DIM_COMMUNE`
- Un traitement peut être nécessaire pour gérer les arrondissements (ex: `75101` → `75056`)
- La clé `commune_sk` est nullable car certaines structures peuvent ne pas avoir de géolocalisation

### Règles de qualité data·inclusion

- **SIRET** : vérifié régulièrement, les codes invalides sont retirés
- **E-mails** : format RFC 5322 + vérification du destinataire, les adresses invalides sont supprimées
- **Codes géographiques** : validés via référentiel INSEE

---

## Justifications de la modélisation

### Approche table unique vs 3NF

**Choix retenu** : Table unique `SIAE_STRUCTURE` avec colonne `extras`

**Avantages** :
- ✅ Simplicité : une seule table à maintenir
- ✅ Performance : pas de jointures multiples pour accès aux données courantes
- ✅ Flexibilité : ajout de nouveaux attributs dans `extras` sans migration de schéma
- ✅ Évolutivité : promotion progressive des champs fréquemment utilisés

**Alternative 3NF** (non retenue pour l'instant) :
- Séparation en `DIM_SOURCE_DATA_INCLUSION`, `DIM_RESEAU_PORTEUR`, `FACT_STRUCTURE_RESEAU`
- À envisager si les analyses par réseau porteur deviennent intensives

### Conventions de nommage respectées

- ✅ Préfixe `siae_` pour distinguer des autres structures
- ✅ Suffixe `_sk` pour les clés de substitution
- ✅ Suffixe `_code` pour les codes officiels (SIRET, INSEE)
- ✅ Suffixe `_label` pour les libellés
- ✅ Pas d'abréviations dans les concepts métier
- ✅ 4 champs de métadonnées obligatoires : `job_insert_id`, `job_insert_date_utc`, `job_update_id`, `job_update_date_utc`
- ✅ Colonne `extras` (VARIANT) pour pattern core/extras

---

## Diagramme de relations

```
┌─────────────────────┐
│  SIAE_STRUCTURE     │
├─────────────────────┤
│ siae_structure_sk ◄─┼── PK
│ siae_structure_bk   │
│ siae_structure_...  │
│ commune_sk ─────────┼──► DIM_COMMUNE (MDM)
│ ...                 │
│ extras (VARIANT)    │
│   ├─ source         │
│   ├─ code_postal    │
│   ├─ date_maj       │
│   ├─ horaires_...   │
│   ├─ accessibi...   │
│   └─ reseaux_por... │
└─────────────────────┘
```
