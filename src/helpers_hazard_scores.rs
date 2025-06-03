use axum::{
    Json,
    http::StatusCode,
};
use serde_json::Value;
use sqlx::Row;

use crate::structs_hazard_scores;

// HELPERS

pub async fn batch_hazard_scores(
    pool: &sqlx::Pool<sqlx::Postgres>,
    points: Vec<structs_hazard_scores::HazardPoint>,
    sql: &str,
) -> Result<Vec<structs_hazard_scores::HazardScore>, (StatusCode, Json<Value>)> {
    // 1) empty array check
    if points.is_empty() {
        let payload = serde_json::json!({ "error": "No points provided" });
        return Err((StatusCode::BAD_REQUEST, Json(payload)));
    }

    // 2) Serialize to JSONB
    let points_json = serde_json::to_value(&points)
        .map_err(|err| {
            eprintln!("Serialization error: {}", err);
            let payload = serde_json::json!({ "error": "Invalid point data" });
            (StatusCode::BAD_REQUEST, Json(payload))
        })?;

    // 3) run the query (SRID is hardcoded; could also be passed in)
    let rows = sqlx::query(sql)
        .bind(points_json)
        .bind(4326)
        .fetch_all(pool)
        .await
        .map_err(|err| {
            eprintln!("DB error (hazard batch scoring): {}", err);
            let payload = serde_json::json!({ "error": "Database error" });
            (StatusCode::INTERNAL_SERVER_ERROR, Json(payload))
        })?;

    // 4) map to output struct
    let scores = rows
        .into_iter()
        .filter_map(|row| {
            Some(structs_hazard_scores::HazardScore {
                id: row.try_get("id").ok()?,
                score: row.try_get("score").ok()?,
            })
        })
        .collect();

    Ok(scores)
}
