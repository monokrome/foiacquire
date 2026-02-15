//! Entity search and browse API endpoints.

use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use super::super::AppState;
use super::api_types::ApiResponse;
use super::helpers::{bad_request, internal_error, not_found, paginate, PaginatedResponse};
use foia::repository::diesel_document::entities::EntityFilter;
#[cfg(feature = "gis")]
use foia::services::geolookup;

/// Query parameters for entity search.
#[derive(Debug, Deserialize, IntoParams)]
pub struct EntitySearchQuery {
    /// Entity text search (LIKE %q%)
    pub q: Option<String>,
    /// Filter by entity type (person, organization, location, file_number)
    pub entity_type: Option<String>,
    /// Exact match (default: false)
    pub exact: Option<bool>,
    /// Additional "type:text" filter pairs, comma-separated
    pub filters: Option<String>,
    /// Raw coordinates: "lat,lon,radius_km"
    pub near: Option<String>,
    /// Named location: "Moscow,100km" -- resolved via geolookup (requires gis feature)
    pub near_location: Option<String>,
    /// Filter by source
    pub source: Option<String>,
    /// Page number (1-indexed)
    pub page: Option<usize>,
    /// Items per page (default: 50, max: 200)
    pub per_page: Option<usize>,
}

/// Query parameters for top entities.
#[derive(Debug, Deserialize, IntoParams)]
pub struct TopEntitiesQuery {
    /// Entity type to get top entries for
    pub entity_type: Option<String>,
    /// Limit (default: 20)
    pub limit: Option<usize>,
}

/// Query parameters for location listing.
#[derive(Debug, Deserialize, IntoParams)]
pub struct LocationsQuery {
    pub page: Option<usize>,
    pub per_page: Option<usize>,
}

/// A matched entity in search results.
#[derive(Debug, Serialize, ToSchema)]
pub struct MatchedEntity {
    pub entity_type: String,
    pub entity_text: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
}

/// Single entity search result with document info and matched entities.
#[derive(Debug, Serialize, ToSchema)]
pub struct EntitySearchResult {
    pub document_id: String,
    pub title: String,
    pub source_id: String,
    pub matched_entities: Vec<MatchedEntity>,
}

/// Entity type count for /api/entities/types.
#[derive(Debug, Serialize, ToSchema)]
pub struct EntityTypeStats {
    pub entity_type: String,
    pub count: u64,
}

/// Top entity for /api/entities/top.
#[derive(Debug, Serialize, ToSchema)]
pub struct TopEntity {
    pub entity_text: String,
    pub document_count: u64,
}

/// Geocoded location for /api/entities/locations.
#[derive(Debug, Serialize, ToSchema)]
pub struct GeocodedLocation {
    pub entity_text: String,
    pub latitude: f64,
    pub longitude: f64,
    pub document_id: String,
}

/// Search documents by entity filters.
#[utoipa::path(
    get,
    path = "/api/entities/search",
    params(EntitySearchQuery),
    responses(
        (status = 200, description = "Paginated entity search results", body = PaginatedResponse<EntitySearchResult>),
        (status = 400, description = "Missing search parameters")
    ),
    tag = "Entities"
)]
pub async fn search_entities(
    State(state): State<AppState>,
    Query(params): Query<EntitySearchQuery>,
) -> impl IntoResponse {
    if let Some(near_str) = &params.near {
        return handle_near_query(&state, near_str, &params).await;
    }

    if let Some(near_loc) = &params.near_location {
        #[cfg(feature = "gis")]
        {
            return handle_near_location_query(&state, near_loc, &params).await;
        }
        #[cfg(not(feature = "gis"))]
        {
            let _ = near_loc;
            return bad_request("near_location requires the 'gis' feature").into_response();
        }
    }

    let mut filters = Vec::new();

    if let Some(q) = &params.q {
        if !q.is_empty() {
            filters.push(EntityFilter {
                entity_type: params.entity_type.clone(),
                text: q.clone(),
                exact: params.exact.unwrap_or(false),
            });
        }
    }

    if let Some(filter_str) = &params.filters {
        for pair in filter_str.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            if let Some((entity_type, text)) = pair.split_once(':') {
                filters.push(EntityFilter {
                    entity_type: Some(entity_type.trim().to_string()),
                    text: text.trim().to_string(),
                    exact: params.exact.unwrap_or(false),
                });
            } else {
                filters.push(EntityFilter {
                    entity_type: None,
                    text: pair.to_string(),
                    exact: params.exact.unwrap_or(false),
                });
            }
        }
    }

    if filters.is_empty() {
        return bad_request(
            "At least one search parameter (q, filters, near, near_location) is required",
        )
        .into_response();
    }

    let (page, per_page, offset) = paginate(params.page, params.per_page);

    let total = match state
        .doc_repo
        .count_by_entities(&filters, params.source.as_deref())
        .await
    {
        Ok(c) => c,
        Err(e) => return internal_error(e).into_response(),
    };

    let doc_ids = match state
        .doc_repo
        .search_by_entities(&filters, params.source.as_deref(), per_page, offset)
        .await
    {
        Ok(ids) => ids,
        Err(e) => return internal_error(e).into_response(),
    };

    let items = match build_search_results(&state, &doc_ids).await {
        Ok(items) => items,
        Err(e) => return internal_error(e).into_response(),
    };

    Json(PaginatedResponse::new(items, page, per_page, total)).into_response()
}

/// Get entity type breakdown with counts.
#[utoipa::path(
    get,
    path = "/api/entities/types",
    responses(
        (status = 200, description = "Entity type counts", body = Vec<EntityTypeStats>)
    ),
    tag = "Entities"
)]
pub async fn entity_types(State(state): State<AppState>) -> impl IntoResponse {
    match state.doc_repo.get_entity_type_counts().await {
        Ok(counts) => {
            let stats: Vec<EntityTypeStats> = counts
                .into_iter()
                .map(|(entity_type, count)| EntityTypeStats { entity_type, count })
                .collect();
            ApiResponse::ok(stats).into_response()
        }
        Err(e) => internal_error(e).into_response(),
    }
}

/// Get most frequent entities by type.
#[utoipa::path(
    get,
    path = "/api/entities/top",
    params(TopEntitiesQuery),
    responses(
        (status = 200, description = "Top entities", body = Vec<TopEntity>)
    ),
    tag = "Entities"
)]
pub async fn top_entities(
    State(state): State<AppState>,
    Query(params): Query<TopEntitiesQuery>,
) -> impl IntoResponse {
    let entity_type = params.entity_type.as_deref().unwrap_or("organization");
    let limit = params.limit.unwrap_or(20).clamp(1, 100);

    match state.doc_repo.get_top_entities(entity_type, limit).await {
        Ok(top) => {
            let items: Vec<TopEntity> = top
                .into_iter()
                .map(|(entity_text, document_count)| TopEntity {
                    entity_text,
                    document_count,
                })
                .collect();
            ApiResponse::ok(items).into_response()
        }
        Err(e) => internal_error(e).into_response(),
    }
}

/// Get all entities for a specific document.
#[utoipa::path(
    get,
    path = "/api/documents/{doc_id}/entities",
    params(("doc_id" = String, Path, description = "Document ID")),
    responses(
        (status = 200, description = "Document entities", body = Vec<MatchedEntity>),
        (status = 404, description = "Document not found")
    ),
    tag = "Entities"
)]
pub async fn document_entities(
    State(state): State<AppState>,
    Path(doc_id): Path<String>,
) -> impl IntoResponse {
    match state.doc_repo.get(&doc_id).await {
        Ok(None) => return not_found("Document not found").into_response(),
        Err(e) => return internal_error(e).into_response(),
        Ok(Some(_)) => {}
    }

    match state.doc_repo.get_document_entities(&doc_id).await {
        Ok(entities) => {
            let items: Vec<MatchedEntity> = entities
                .into_iter()
                .map(|e| MatchedEntity {
                    entity_type: e.entity_type,
                    entity_text: e.entity_text,
                    latitude: e.latitude,
                    longitude: e.longitude,
                })
                .collect();
            ApiResponse::ok(items).into_response()
        }
        Err(e) => internal_error(e).into_response(),
    }
}

/// Get geocoded locations with coordinates (for map views).
#[utoipa::path(
    get,
    path = "/api/entities/locations",
    params(LocationsQuery),
    responses(
        (status = 200, description = "Geocoded locations", body = PaginatedResponse<GeocodedLocation>)
    ),
    tag = "Entities"
)]
pub async fn entity_locations(
    State(state): State<AppState>,
    Query(params): Query<LocationsQuery>,
) -> impl IntoResponse {
    let (page, per_page, offset) = paginate(params.page, params.per_page);

    let total = match state.doc_repo.count_geocoded_entities().await {
        Ok(c) => c,
        Err(e) => return internal_error(e).into_response(),
    };

    let entities = match state.doc_repo.get_geocoded_entities(per_page, offset).await {
        Ok(e) => e,
        Err(e) => return internal_error(e).into_response(),
    };

    let items: Vec<GeocodedLocation> = entities
        .into_iter()
        .map(|e| GeocodedLocation {
            entity_text: e.entity_text,
            latitude: e.latitude.unwrap_or(0.0),
            longitude: e.longitude.unwrap_or(0.0),
            document_id: e.document_id,
        })
        .collect();

    Json(PaginatedResponse::new(items, page, per_page, total)).into_response()
}

async fn handle_near_query(
    state: &AppState,
    near_str: &str,
    params: &EntitySearchQuery,
) -> axum::response::Response {
    let parts: Vec<&str> = near_str.split(',').collect();
    if parts.len() != 3 {
        return bad_request("Invalid 'near' format. Expected: lat,lon,radius_km").into_response();
    }

    let lat: f64 = match parts[0].trim().parse() {
        Ok(v) => v,
        Err(_) => return bad_request("Invalid latitude in 'near'").into_response(),
    };
    let lon: f64 = match parts[1].trim().parse() {
        Ok(v) => v,
        Err(_) => return bad_request("Invalid longitude in 'near'").into_response(),
    };
    let radius_km: f64 = match parts[2].trim().parse() {
        Ok(v) => v,
        Err(_) => return bad_request("Invalid radius in 'near'").into_response(),
    };

    let (page, per_page, offset) = paginate(params.page, params.per_page);

    let total = match state
        .doc_repo
        .count_near_location(lat, lon, radius_km)
        .await
    {
        Ok(c) => c,
        Err(e) => return internal_error(e).into_response(),
    };

    let doc_ids = match state
        .doc_repo
        .search_near_location(lat, lon, radius_km, per_page, offset)
        .await
    {
        Ok(ids) => ids,
        Err(e) => return internal_error(e).into_response(),
    };

    let items = match build_search_results(state, &doc_ids).await {
        Ok(items) => items,
        Err(e) => return internal_error(e).into_response(),
    };

    Json(PaginatedResponse::new(items, page, per_page, total)).into_response()
}

#[cfg(feature = "gis")]
async fn handle_near_location_query(
    state: &AppState,
    near_loc: &str,
    params: &EntitySearchQuery,
) -> axum::response::Response {
    let parts: Vec<&str> = near_loc.rsplitn(2, ',').collect();
    if parts.len() != 2 {
        return bad_request(
            "Invalid 'near_location' format. Expected: location_name,radius_km (e.g., Moscow,100)",
        )
        .into_response();
    }

    let radius_str = parts[0].trim();
    let location_name = parts[1].trim();

    let radius_km: f64 = match radius_str.parse() {
        Ok(v) => v,
        Err(_) => return bad_request("Invalid radius in 'near_location'").into_response(),
    };

    let (lat, lon) = match geolookup::lookup(location_name) {
        Some(coords) => coords,
        None => {
            return bad_request(&format!(
                "Unknown location: '{}'. Use 'near' parameter with explicit lat,lon,radius_km instead.",
                location_name
            ))
            .into_response();
        }
    };

    let (page, per_page, offset) = paginate(params.page, params.per_page);

    let total = match state
        .doc_repo
        .count_near_location(lat, lon, radius_km)
        .await
    {
        Ok(c) => c,
        Err(e) => return internal_error(e).into_response(),
    };

    let doc_ids = match state
        .doc_repo
        .search_near_location(lat, lon, radius_km, per_page, offset)
        .await
    {
        Ok(ids) => ids,
        Err(e) => return internal_error(e).into_response(),
    };

    let items = match build_search_results(state, &doc_ids).await {
        Ok(items) => items,
        Err(e) => return internal_error(e).into_response(),
    };

    Json(PaginatedResponse::new(items, page, per_page, total)).into_response()
}

async fn build_search_results(
    state: &AppState,
    doc_ids: &[String],
) -> Result<Vec<EntitySearchResult>, Box<dyn std::error::Error + Send + Sync>> {
    if doc_ids.is_empty() {
        return Ok(vec![]);
    }

    let entities_map = state
        .doc_repo
        .get_entities_batch(doc_ids)
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    let mut results = Vec::with_capacity(doc_ids.len());
    for id in doc_ids {
        let (title, source_id) = match state.doc_repo.get(id).await {
            Ok(Some(doc)) => (doc.title, doc.source_id),
            _ => (id.clone(), String::new()),
        };

        let matched_entities = entities_map
            .get(id)
            .map(|es| {
                es.iter()
                    .map(|e| MatchedEntity {
                        entity_type: e.entity_type.clone(),
                        entity_text: e.entity_text.clone(),
                        latitude: e.latitude,
                        longitude: e.longitude,
                    })
                    .collect()
            })
            .unwrap_or_default();

        results.push(EntitySearchResult {
            document_id: id.clone(),
            title,
            source_id,
            matched_entities,
        });
    }

    Ok(results)
}
