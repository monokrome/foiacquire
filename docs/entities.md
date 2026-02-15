# Entity Extraction & Spatial Search

Named entity recognition (NER) extracts people, organizations, locations, and file numbers from document text. Extracted entities are stored in a dedicated table for fast, indexed querying across your entire document collection.

## Setup

### 1. Run migrations

```bash
foia db migrate
```

This creates the `document_entities` table on both SQLite and PostgreSQL.

### 2. Extract entities

```bash
foia extract-entities [source_id] [-l limit]
```

This scans documents that have extracted text, runs regex-based NER, and populates the entity table. With the `gis` feature enabled, location entities are automatically geocoded using an embedded database of ~25,000 cities.

Entities are also populated automatically when running `foia annotate` via the NER annotator's post-processing hook.

### 3. (Optional) Load region boundaries

For spatial queries like "documents mentioning locations in France":

```bash
foia db load-regions
```

This requires PostgreSQL with PostGIS. It loads Natural Earth country and state/province boundaries into a `regions` table. Safe to run repeatedly.

## Entity Types

| Type | Description | Examples |
|------|-------------|----------|
| `organization` | Government agencies, companies | FBI, CIA, Department of Defense |
| `person` | Named individuals | Director Mueller, Secretary Powell |
| `location` | Places, cities, facilities | Fort Meade, Moscow, Area 51 |
| `file_number` | Case/file identifiers | FOIA-2024-00123, Case No. 19-cv-1234 |

## CLI Usage

### Search by entity text

```bash
foia search-entities CIA
foia search-entities "Fort Meade" --type location
foia search-entities Mueller --type person --source fbi_vault
```

### Spatial search (PostgreSQL + PostGIS)

Search by raw coordinates with a radius in kilometers:

```bash
foia search-entities Moscow --near "55.75,37.61,100"
```

### Backfill from existing annotations

If you've already run `extract-entities` before the entity table existed:

```bash
foia backfill-entities [source_id] [-l limit]
```

This reads NER results from document metadata JSON and populates entity rows.

## HTTP API

All entity endpoints return JSON.

### Search entities

```
GET /api/entities/search?q=CIA&entity_type=organization
GET /api/entities/search?q=Moscow&entity_type=location
GET /api/entities/search?near=55.75,37.61,100
GET /api/entities/search?near_location=Moscow,100
GET /api/entities/search?filters=organization:FBI,person:Mueller
```

| Parameter | Description |
|-----------|-------------|
| `q` | Entity text search (substring match) |
| `entity_type` | Filter: `person`, `organization`, `location`, `file_number` |
| `exact` | Exact match instead of substring (default: false) |
| `filters` | Multiple `type:text` pairs, comma-separated (AND logic) |
| `near` | Raw coordinates: `lat,lon,radius_km` |
| `near_location` | Named location: `name,radius_km` (requires `gis` feature) |
| `source` | Filter by source ID |
| `page` | Page number (1-indexed) |
| `per_page` | Results per page (default: 50, max: 200) |

### Entity type breakdown

```
GET /api/entities/types
```

Returns counts per entity type.

### Top entities

```
GET /api/entities/top?entity_type=person&limit=20
```

Most frequently occurring entities, optionally filtered by type.

### Document entities

```
GET /api/documents/:doc_id/entities
```

All entities for a specific document.

### Geocoded locations

```
GET /api/entities/locations?page=1&per_page=100
```

All entities with lat/lng coordinates. Useful for map views.

## GIS Feature

The `gis` feature flag controls whether geographic data is compiled into the binary:

| Component | With `gis` | Without `gis` |
|-----------|-----------|---------------|
| City geocoding (~25K cities) | Locations get lat/lng | Locations have null coordinates |
| Region boundaries | `db load-regions` available | Command hidden |
| `near_location` API param | Resolves names to coords | Returns error |
| `near` API param | Works (no embedded data needed) | Works |
| Entity text search | Works | Works |

The `gis` feature adds ~9MB to the binary (GeoNames city data + Natural Earth GeoJSON). It's enabled by default in Docker builds.

## Database Backends

| Feature | SQLite | PostgreSQL | PostgreSQL + PostGIS |
|---------|--------|------------|---------------------|
| Entity text search | Yes | Yes | Yes |
| Entity type/top queries | Yes | Yes | Yes |
| `near` (coordinate search) | Error | Error | Yes |
| `near_location` (named) | Error | Error | Yes |
| Region polygon queries | Error | Error | Yes |

Spatial queries on unsupported backends return a clear error message rather than failing silently.
