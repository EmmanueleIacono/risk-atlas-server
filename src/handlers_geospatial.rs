use axum::{
    body::Body,
    extract::{Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::Value;

use crate::structs_geospatial;
use crate::helpers_geospatial;
use crate::AppState;

// HANDLERS

pub async fn get_osm_buildings_handler(
    Query(q): Query<structs_geospatial::BBoxQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // 0. parse bbox
    let parts: Vec<f64> = q.bbox.split(",").filter_map(|s| s.parse::<f64>().ok()).collect();
    if parts.len() != 4 {
        return (StatusCode::BAD_REQUEST, "bbox must be 'minLon,minLat,maxLon,maxLat'").into_response();
    }
    let (west, south, east, north) = (parts[0], parts[1], parts[2], parts[3]);

    // 1. building Overpass query
    let query = format!(
        "\
        [out:json][timeout:25];\
        (\
            way[\"building\"]({s},{w},{n},{e});\
            relation[\"building\"]({s},{w},{n},{e});\
        );\
        out body geom;\
        ",
        s=south, w=west, n=north, e=east
    );

    // 2. forward query to Overpass API
    let resp = match state.client
        .post("https://overpass-api.de/api/interpreter")
        .body(query)
        .send()
        .await
    {
        Ok(r) => r,
        Err(err) => {
            eprintln!("Error contacting Overpass API: {}", err);
            return (StatusCode::BAD_GATEWAY, "Bad Gateway").into_response();
        }
    };

    // 3. parse JSON
    let osm_json: Value = match resp.json().await {
        Ok(json) => json,
        Err(err) => {
            eprintln!("Error parsing Overpass response: {}", err);
            return (StatusCode::BAD_GATEWAY, "Invalid Overpass response").into_response();
        }
    };

    // 4. convert to GeoJSON Features
    let fc = helpers_geospatial::osm_to_geojson(&osm_json);

    // 5. encode to FlatGeobuf
    let buf = match helpers_geospatial::geojson_to_flatgeobuf(&fc) {
        Ok(data) => data,
        Err(err) => {
            eprintln!("Error encoding Flatgeobuf: {}", err);
            return (StatusCode::INTERNAL_SERVER_ERROR, "Encoding error").into_response();
        }
    };

    // 6. building Response with FlatGeobuf headers
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-flatgeobuf")
        .header(header::ACCEPT_RANGES, "bytes")
        .body(Body::from(buf))
        .unwrap()
}
