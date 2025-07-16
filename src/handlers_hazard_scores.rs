use axum::{
    extract::State,
    response::IntoResponse,
    Json,
    http::StatusCode,
};

use crate::structs_hazard_scores;
use crate::helpers_hazard_scores;
use crate::AppState;

// HANDLERS

/// Handler for batch flood hazard scoring
/// Expects JSON array of HazardPoint, e.g. [{"id":1,"lon":12.49,"lat":41.89}, ...]
pub async fn get_flood_hazard_batch_scores_handler(
    State(state): State<AppState>,
    Json(points): Json<Vec<structs_hazard_scores::HazardPoint>>,
) -> impl IntoResponse {
    // Call the batch scoring function in PostGIS
    // let sql = r#"
    //     SELECT id, score
    //     FROM gis.score_hazard_batch($1::jsonb, $2, 'hazard_flood')
    // "#;
    let sql = r#"
        SELECT id, score
        FROM gis.score_hazard_batch_v3($1::jsonb, $2, 'mv_hazard_flood_segmented')
    "#;

    match helpers_hazard_scores::batch_hazard_scores(&state.pool, points, sql).await {
        Ok(scores) => (StatusCode::OK, Json(scores)).into_response(),
        Err((status, json)) => (status, json).into_response(),
    }
}

/// Handler for batch landslide hazard scoring
/// Expects JSON array of HazardPoint, e.g. [{"id":1,"lon":12.49,"lat":41.89}, ...]
pub async fn get_landslide_hazard_batch_scores_handler(
    State(state): State<AppState>,
    Json(points): Json<Vec<structs_hazard_scores::HazardPoint>>,
) -> impl IntoResponse {
    // Call the batch scoring function in PostGIS
    let sql = r#"
        SELECT id, score
        FROM gis.score_hazard_batch_v3($1::jsonb, $2, 'mv_hazard_landslide_segmented')
    "#;

    match helpers_hazard_scores::batch_hazard_scores(&state.pool, points, sql).await {
        Ok(scores) => (StatusCode::OK, Json(scores)).into_response(),
        Err((status, json)) => (status, json).into_response(),
    }
}

/// Handler for batch seismic hazard scoring
/// Expects JSON array of HazardPoint, e.g. [{"id":1,"lon":12.49,"lat":41.89}, ...]
pub async fn get_seismic_hazard_batch_scores_handler(
    State(state): State<AppState>,
    Json(points): Json<Vec<structs_hazard_scores::HazardPoint>>,
) -> impl IntoResponse {
    // Call the batch scoring function in PostGIS
    let sql = r#"
        SELECT id, score
        FROM gis.score_seismic_hazard_batch_v2($1::jsonb, $2)
    "#;

    match helpers_hazard_scores::batch_hazard_scores(&state.pool, points, sql).await {
        Ok(scores) => (StatusCode::OK, Json(scores)).into_response(),
        Err((status, json)) => (status, json).into_response(),
    }
}
