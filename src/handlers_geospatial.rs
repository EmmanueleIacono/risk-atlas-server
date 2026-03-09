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

// CONSTS

// !!! #1) the ::text casts needs to be explicit, even with varchar attributes, in order to work
// !!! #2) currently only the ::text cast works; ::numeric or others do not

const REGIONS_SQL: &str = r#"
    WITH bbox AS (
        SELECT ST_Transform(
            ST_MakeEnvelope($1, $2, $3, $4, $5),
            ST_SRID(geom)
        ) AS bbox
        FROM gis.admin_bounds_italy
        LIMIT 1
    ), feats AS (
        SELECT
            ST_Transform(
                ST_Multi(
                    ST_CollectionExtract(geom, 3)
                ),
                4326
            ) AS geom,
            den_reg::text AS "Denominazione",
            COALESCE(ROUND((shape_leng / 1000)::numeric, 2), 0)::text AS "Lunghezza confini (km)",
            COALESCE(ROUND((shape_area / 1000000)::numeric, 3), 0)::text AS "Superficie (kmq)"
        FROM gis.admin_bounds_italy, bbox
        WHERE geom && bbox.bbox
        AND ST_Intersects(geom, bbox.bbox)
        AND tipo = 'R'
    )
    SELECT ST_AsFlatGeobuf(feats, TRUE, 'geom') AS fgb
    FROM feats;
"#;

const PROVINCES_SQL: &str = r#"
    WITH bbox AS (
        SELECT ST_Transform(
            ST_MakeEnvelope($1, $2, $3, $4, $5),
            ST_SRID(geom)
        ) AS bbox
        FROM gis.admin_bounds_italy
        LIMIT 1
    ), feats AS (
        SELECT
            ST_Transform(
                ST_Multi(
                    ST_CollectionExtract(geom, 3)
                ),
                4326
            ) AS geom,
            den_uts::text AS "Denominazione",
            sigla::text AS "Sigla",
            tipo_uts::text AS "Tipologia",
            COALESCE(ROUND((shape_leng / 1000)::numeric, 2), 0)::text AS "Lunghezza confini (km)",
            COALESCE(ROUND((shape_area / 1000000)::numeric, 3), 0)::text AS "Superficie (kmq)"
        FROM gis.admin_bounds_italy, bbox
        WHERE geom && bbox.bbox
        AND ST_Intersects(geom, bbox.bbox)
        AND tipo = 'P'
    )
    SELECT ST_AsFlatGeobuf(feats, TRUE, 'geom') AS fgb
    FROM feats;
"#;

const MUNICIPALITIES_SQL: &str = r#"
    WITH bbox AS (
        SELECT ST_Transform(
            ST_MakeEnvelope($1, $2, $3, $4, $5),
            ST_SRID(geom)
        ) AS bbox
        FROM gis.admin_bounds_italy
        LIMIT 1
    ), feats AS (
        SELECT
            ST_Transform(
                ST_Multi(
                    ST_CollectionExtract(geom, 3)
                ),
                4326
            ) AS geom,
            COALESCE(comune::text, '') AS "Comune",
            COALESCE(comune_a::text, '') AS "Altre denominazioni",
            COALESCE(ROUND((shape_leng / 1000)::numeric, 2), 0)::text AS "Lunghezza confini (km)",
            COALESCE(ROUND((shape_area / 1000000)::numeric, 3), 0)::text AS "Superficie (kmq)"
        FROM gis.admin_bounds_italy, bbox
        WHERE geom && bbox.bbox
        AND ST_Intersects(geom, bbox.bbox)
        AND tipo = 'C'
    )
    SELECT ST_AsFlatGeobuf(feats, TRUE, 'geom') AS fgb
    FROM feats;
"#;

const WATER_DISTRICTS_SQL: &str = r#"
    WITH bbox AS (
        SELECT ST_Transform(
            ST_MakeEnvelope($1, $2, $3, $4, $5),
            ST_SRID(geom)
        ) AS bbox
        FROM gis.italian_water_districts
        LIMIT 1
    ), feats AS (
        SELECT
            geom,
            uuid::text AS uuid,
            district::text AS district,
            eu_code::text AS eu_code
        FROM gis.italian_water_districts, bbox
        WHERE geom && bbox.bbox
        AND ST_Intersects(geom, bbox.bbox)
    )
    SELECT ST_AsFlatGeobuf(feats, TRUE, 'geom') AS fgb
    FROM feats;
"#;

const HAZARD_FLOODING_AREAS_SQL: &str = r#"
    WITH bbox AS (
        SELECT ST_Transform(
            ST_MakeEnvelope($1, $2, $3, $4, $5),
            ST_SRID(geom)
        ) AS bbox
        FROM gis.mv_hazard_flood_segmented
        LIMIT 1
    ), feats AS (
        SELECT
            ST_Transform(
                ST_Multi(
                    ST_CollectionExtract(geom, 3)
                ),
                4326
            ) AS geom,
            scenario::text AS "Scenario",
            scenario_code::text AS "Scenario code"
        FROM gis.mv_hazard_flood_segmented, bbox
        WHERE geom && bbox.bbox
        AND ST_Intersects(geom, bbox.bbox)
    )
    SELECT ST_AsFlatGeobuf(feats, TRUE, 'geom') AS fgb
    FROM feats;
"#;

const HAZARD_LANDSLIDE_AREAS_SQL: &str = r#"
    WITH bbox AS (
        SELECT ST_Transform(
            ST_MakeEnvelope($1, $2, $3, $4, $5),
            ST_SRID(geom)
        ) AS bbox
        FROM gis.mv_hazard_landslide_segmented
        LIMIT 1
    ), feats AS (
        SELECT
            ST_Transform(
                ST_Multi(
                    ST_CollectionExtract(geom, 3)
                ),
                4326
            ) AS geom,
            scenario::text AS "Scenario",
            scenario_code::text AS "Scenario code"
        FROM gis.mv_hazard_landslide_segmented, bbox
        WHERE geom && bbox.bbox
        AND ST_Intersects(geom, bbox.bbox)
    )
    SELECT ST_AsFlatGeobuf(feats, TRUE, 'geom') AS fgb
    FROM feats;
"#;

const HAZARD_PGA_POINTS_SQL: &str = r#"
    WITH bbox AS (
        SELECT ST_Transform(
            ST_MakeEnvelope($1, $2, $3, $4, $5),
            ST_SRID(geom)
        ) AS bbox
        FROM gis.italian_peak_ground_acceleration
        LIMIT 1
    ), feats AS (
        SELECT
            ST_Transform(geom, 4326) AS geom,
            id::text AS "ID",
            lon::text AS "Longitude",
            lat::text AS "Latitude",
            ag::text AS "Peak Ground Acceleration - standard (%g)",
            perc::text AS "Peak Ground Acceleration - 16th percentile (%g)",
            perc_1::text AS "Peak Ground Acceleration - 84th percentile (%g)"
        FROM gis.italian_peak_ground_acceleration, bbox
        WHERE geom && bbox.bbox
        AND ST_Intersects(geom, bbox.bbox)
    )
    SELECT ST_AsFlatGeobuf(feats, TRUE, 'geom') AS fgb
    FROM feats;
"#;

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

pub async fn get_admin_bounds_regions_fgb_handler(
    Query(q): Query<structs_geospatial::BBoxQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // split bbox
    let (min_x, min_y, max_x, max_y) = match helpers_geospatial::parse_bbox(&q.bbox) {
        Ok(t) => t,
        Err(resp) => return resp.into_response(),
    };

    // fetch the data
    helpers_geospatial::fetch_fgb(min_x, min_y, max_x, max_y, q.epsg, &state.pool, REGIONS_SQL)
        .await
}

pub async fn get_admin_bounds_provinces_fgb_handler(
    Query(q): Query<structs_geospatial::BBoxQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // split bbox
    let (min_x, min_y, max_x, max_y) = match helpers_geospatial::parse_bbox(&q.bbox) {
        Ok(t) => t,
        Err(resp) => return resp.into_response(),
    };

    // fetch the data
    helpers_geospatial::fetch_fgb(min_x, min_y, max_x, max_y, q.epsg, &state.pool, PROVINCES_SQL)
        .await
}

pub async fn get_admin_bounds_municipalities_fgb_handler(
    Query(q): Query<structs_geospatial::BBoxQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // split bbox
    let (min_x, min_y, max_x, max_y) = match helpers_geospatial::parse_bbox(&q.bbox) {
        Ok(t) => t,
        Err(resp) => return resp.into_response(),
    };

    // fetch the data
    helpers_geospatial::fetch_fgb(min_x, min_y, max_x, max_y, q.epsg, &state.pool, MUNICIPALITIES_SQL)
        .await
}

pub async fn get_water_districts_fgb_handler(
    Query(q): Query<structs_geospatial::BBoxQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // split bbox
    let (min_x, min_y, max_x, max_y) = match helpers_geospatial::parse_bbox(&q.bbox) {
        Ok(t) => t,
        Err(resp) => return resp.into_response(),
    };

    // fetch the data
    helpers_geospatial::fetch_fgb(min_x, min_y, max_x, max_y, q.epsg, &state.pool, WATER_DISTRICTS_SQL)
        .await
}

pub async fn get_hazard_flooding_areas_fgb_handler(
    Query(q): Query<structs_geospatial::BBoxQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // split bbox
    let (min_x, min_y, max_x, max_y) = match helpers_geospatial::parse_bbox(&q.bbox) {
        Ok(t) => t,
        Err(resp) => return resp.into_response(),
    };

    // fetch the data
    helpers_geospatial::fetch_fgb(min_x, min_y, max_x, max_y, q.epsg, &state.pool, HAZARD_FLOODING_AREAS_SQL)
        .await
}

pub async fn get_hazard_landslide_areas_fgb_handler(
    Query(q): Query<structs_geospatial::BBoxQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // split bbox
    let (min_x, min_y, max_x, max_y) = match helpers_geospatial::parse_bbox(&q.bbox) {
        Ok(t) => t,
        Err(resp) => return resp.into_response(),
    };

    // fetch the data
    helpers_geospatial::fetch_fgb(min_x, min_y, max_x, max_y, q.epsg, &state.pool, HAZARD_LANDSLIDE_AREAS_SQL)
        .await
}

pub async fn get_hazard_pga_points_fgb_handler(
    Query(q): Query<structs_geospatial::BBoxQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // split bbox
    let (min_x, min_y, max_x, max_y) = match helpers_geospatial::parse_bbox(&q.bbox) {
        Ok(t) => t,
        Err(resp) => return resp.into_response(),
    };

    // fetch the data
    helpers_geospatial::fetch_fgb(min_x, min_y, max_x, max_y, q.epsg, &state.pool, HAZARD_PGA_POINTS_SQL)
        .await
}
