use std::collections::HashSet;

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use sqlx::Row;
use serde_json::Value;

use crate::AppState;
use crate::helpers;

// STRUCTS
#[derive(Deserialize)]
pub struct TileFilterStr {
    filters: Option<String> // e.g. "IfcSpace;IfcWall"
}

#[derive(Debug, Deserialize)]
pub struct IntersectQuery {
    lat: f64, // latitude (Y)
    lon: f64, // longitude (X)
    epsg: i32, // SRID
}

#[derive(Deserialize)]
pub struct BBoxQuery {
    bbox: String, // "minLon,minLat,maxLon,maxLat" -> e.g. "7.2,44.9,7.8,45.2"
    epsg: i32, // SRID
}

// HANDLERS
pub async fn home_handler() -> impl IntoResponse {
    // returning just an HTML string
    let html_content = r#"<h1>Welcome <i>home</i></h1>"#;
    (StatusCode::OK, html_content)
}

pub async fn get_element_vertices_handler(
    Path((project_id, element_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let query = r#"SELECT * FROM tilesets.vertices
                        WHERE project_id = $1 AND element_id = $2
                        ORDER BY vertex_index ASC"#;

    let rows = sqlx::query(query)
        .bind(&project_id)
        .bind(&element_id)
        .fetch_all(&state.pool)
        .await;

    match rows {
        Ok(rows) => {
            let result_json = rows.into_iter().map(|_row| {
                serde_json::json!({
                    "project_id": _row.try_get::<String, _>("project_id").unwrap_or_default(),
                    "element_id": _row.try_get::<String, _>("element_id").unwrap_or_default(),
                    "vertex_index": _row.try_get::<i32, _>("vertex_index").unwrap_or_default(),
                    // then x, y, z... f64...
                })
            })
            .collect::<Vec<Value>>();

            (StatusCode::OK, Json(result_json)).into_response()
        }
        Err(err) => {
            eprintln!("Error while fetching element vertices: {}", err);
            (StatusCode::INTERNAL_SERVER_ERROR, "Database error").into_response()
        }
    }
}

pub async fn get_available_ifc_classes(
    State(state): State<AppState>
) -> impl IntoResponse {
    let query = r#"SELECT project_id, ARRAY_AGG(DISTINCT(ifc_class)) AS ifc_classes
                         FROM tilesets.elements
                         GROUP BY project_id"#;

    let rows = sqlx::query(query)
        .fetch_all(&state.pool)
        .await;

    match rows {
        Ok(rows) => {
            let result_json: Vec<Value> = rows.into_iter().map(|_row| {
                let project_id: String = _row.try_get("project_id").unwrap_or_default();
                let ifc_classes: Vec<String> = _row.try_get("ifc_classes").unwrap_or_default();

                serde_json::json!({
                    "project_id": project_id,
                    "ifc_classes": ifc_classes,
                })
            })
            .collect();
            
            (StatusCode::OK, Json(result_json)).into_response()
        }
        Err(err) => {
            eprintln!("Error while fetching available IFC classes: {}", err);
            (StatusCode::INTERNAL_SERVER_ERROR, "Database error").into_response()
        }
    }
}

pub async fn get_projects_handler(
    State(state): State<AppState>
) -> impl IntoResponse {
    let proj_descr_exists = match helpers::check_project_description(&state.pool).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("DB error: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, "Database error").into_response();
        }
    };
    let query = if proj_descr_exists {
        "SELECT project_id, project_description FROM tilesets.project_data"
    } else {
        "SELECT project_id, NULL AS project_description FROM tilesets.project_data"
    };

    let rows = sqlx::query(query)
        .fetch_all(&state.pool)
        .await;

    match rows {
        Ok(rows) => {
            let result_json: Vec<Value> = rows.into_iter().map(|_row| {
                let project_id: String = _row.try_get("project_id").unwrap_or_default();
                let project_description: Option<String> = _row.try_get("project_description").ok();

                serde_json::json!({
                    "project_id": project_id,
                    "project_description": project_description,
                })
            })
            .collect();
            
            (StatusCode::OK, Json(result_json)).into_response()
        }
        Err(err) => {
            eprintln!("DB error: {}", err);
            (StatusCode::INTERNAL_SERVER_ERROR, "Database error").into_response()
        }
    }
}

pub async fn get_tileset_handler(
    Path(project_id): Path<String>,
    Query(tile_filter_str): Query<TileFilterStr>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    println!("Requested tileset: {}", project_id);

    let allowed_classes: HashSet<String> = tile_filter_str
        .filters
        .as_ref()
        .map(|flt_str| {
            flt_str
                .split(";")
                .map(|st| st.trim().to_string())
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();

    let query = r#"SELECT tileset FROM tilesets.project_data WHERE project_id = $1"#;

    let row = sqlx::query(query)
        .bind(&project_id)
        .fetch_one(&state.pool)
        .await;

    match row {
        Ok(row) => {
            let mut tileset_data: serde_json::Value = row.try_get("tileset").unwrap_or(Value::Null);
            if tileset_data.is_null() {
                return (StatusCode::NOT_FOUND, "Tileset not found").into_response();
            }

            // if there are filters, apply filtering logic
            if !allowed_classes.is_empty() {
                if let Some(root_node) = tileset_data.get_mut("root") {
                    helpers::filter_tileset(root_node, &allowed_classes);
                }
            }
            (StatusCode::OK, Json(tileset_data)).into_response()
        }
        Err(sqlx::Error::RowNotFound) => {
            (StatusCode::NOT_FOUND, format!("Tileset for project_id '{}' not found.", project_id)).into_response()
        }
        Err(err) => {
            eprintln!("Error while fetching the tileset: {}", err);
            (StatusCode::INTERNAL_SERVER_ERROR, "Database error").into_response()
        }
    }
}

pub async fn get_model_handler(
    Path(gltf_path): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    println!("Requested model: {}", gltf_path);

    let query_path = format!("models/{}", gltf_path);

    let query = r#"SELECT bin_gltf FROM tilesets.elements WHERE gltf_path = $1"#;

    let row = sqlx::query(query)
        .bind(&query_path)
        .fetch_one(&state.pool)
        .await;

    match row {
        Ok(row) => {
            // the column is a byte array
            let bin_gltf: Vec<u8> = row.try_get("bin_gltf").unwrap_or_default();

            if bin_gltf.is_empty() {
                return (StatusCode::NOT_FOUND, "Element binary model not found").into_response();
            }

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "model/gltf-binary")
                .body(Body::from(bin_gltf))
                .unwrap()
        }
        Err(sqlx::Error::RowNotFound) => {
            (
                StatusCode::NOT_FOUND,
                format!("Model for path '{}' not found", gltf_path),
            ).into_response()
        }
        Err(err) => {
            eprintln!("Error while fetching a model: {}", err);
            (StatusCode::INTERNAL_SERVER_ERROR, "Database error").into_response()
        }
    }
}

pub async fn point_intersects_handler(
    Query(params): Query<IntersectQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // NOTE: ST_MakePoint expects (X, Y) -> (lon, lat)!
    let sql = r#"
        SELECT district FROM gis.italian_water_districts
        WHERE ST_Intersects(
            geom,
            ST_Transform(
                ST_SetSRID(ST_MakePoint($1, $2), $3),
                ST_SRID(geom)
            )
        )
    "#;
    
    let rows = sqlx::query(sql)
        .bind(params.lon) // $1
        .bind(params.lat) // $2
        .bind(params.epsg) // $3
        .fetch_all(&state.pool)
        .await;

    match rows {
        Ok(r) => {
            // collecting districts, but keep going if a single row fails to deserialize
            let features: Vec<String> = r.into_iter()
                .filter_map(|row| row.try_get::<String, _>("district").ok())
                .collect();

            let payload = serde_json::json!({
                "intersects": !features.is_empty(),
                "features": features
            });

            (StatusCode::OK, Json(payload)).into_response()
        }
        Err(err) => {
            eprintln!("DB error (intersects): {}", err);
            (StatusCode::INTERNAL_SERVER_ERROR, "Database error").into_response()
        }
    }
}

pub async fn get_districts_fgb_handler(
    Query(q): Query<BBoxQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // split bbox
    let parts: Vec<f64> = q.bbox.split(",").filter_map(|s| s.parse::<f64>().ok()).collect();

    if parts.len() != 4 {
        return (StatusCode::BAD_REQUEST, "bbox must be minLon,minLat,maxLon,maxLat").into_response();
    }

    let (min_x, min_y, max_x, max_y) = (parts[0], parts[1], parts[2], parts[3]);

    // SQL
    let sql = r#"
        WITH bbox AS (
            SELECT ST_Transform(
                ST_MakeEnvelope($1, $2, $3, $4, $5),
                ST_SRID(geom)
            ) AS bbox
            FROM gis.italian_water_districts
            LIMIT 1
        ), feats AS (
            SELECT geom, uuid, district, eu_code
            FROM gis.italian_water_districts, bbox
            WHERE geom && bbox.bbox
            AND ST_Intersects(geom, bbox.bbox)
        )
        SELECT ST_AsFlatGeobuf(feats, TRUE, 'geom') AS fgb
        FROM feats;
    "#;
    // let sql = r#"
    //     SELECT ST_AsFlatGeobuf(distr)
    //     FROM gis.italian_water_districts AS distr;
    // "#; // this causes even more problems (too large fgb? fgb not serialized correctly?) -> try to use flatgeobuf crate to parse it
    // let sql = r#"
    //     WITH prova AS (
    //         SELECT geom, uuid, district, eu_code FROM gis.italian_water_districts LIMIT 1
    //     )
    //     SELECT ST_AsFlatGeobuf(prova)
    //     FROM prova;
    // "#; // THIS WORKS but I'm not returning any properties as of now
    // let sql = r#"
        // WITH test AS (
            // SELECT * FROM gis.italian_water_districts
        // )
        // SELECT ST_AsFlatGeobuf(test)
        // FROM test;
    // "#; // example from: https://www.openstreetmap.org/user/spwoodcock/diary/402948

    let data = sqlx::query_scalar::<_, Option<Vec<u8>>>(sql)
        .bind(min_x)
        .bind(min_y)
        .bind(max_x)
        .bind(max_y)
        .bind(q.epsg)
        .fetch_one(&state.pool)
        .await;

    match data {
        // actual data
        Ok(Some(bin)) => Response::builder()
            .status(StatusCode::OK) // 200
            .header(header::CONTENT_TYPE, "application/x-flatgeobuf")
            .header(header::ACCEPT_RANGES, "bytes")
            .body(Body::from(bin))
            .unwrap(),
        // empty data
        Ok(None) => Response::builder()
            .status(StatusCode::NO_CONTENT) // 204
            .body(Body::empty()) // no body, no type
            .unwrap(),
        // genuine error
        Err(err) => {
            eprintln!("FGB error: {}", err);
            (StatusCode::INTERNAL_SERVER_ERROR, "Database error").into_response() // 500
        }
    }
}
