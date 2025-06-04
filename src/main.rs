use axum::{
    routing::{get, post},
    Router,
};
use tower_http::cors::{Any, CorsLayer};
use sqlx::{Pool, Postgres};
use std::net::{Ipv4Addr, SocketAddr};
use config as config_loader; // rename to avoid conflict with config field name
use once_cell::sync::Lazy;
use anyhow::Result;

mod handlers;
mod handlers_geospatial;
mod handlers_hazard_scores;
mod helpers;
mod helpers_geospatial;
mod helpers_hazard_scores;
mod structs_geospatial;
mod structs_hazard_scores;

// storing config globally (just for demo)
static CONFIG: Lazy<AppConfig> = Lazy::new(|| {
    // loading the config from "config.ini"
    let settings = config_loader::Config::builder()
    .add_source(config_loader::File::with_name("config").required(true))
    .build()
    .expect("Cannot find or read 'config.ini' file.");

    // deserializing into AppConfig struct
    settings
        .try_deserialize::<AppConfig>()
        .expect("Invalid config structure")
});

// custom config struct for config.ini
#[derive(Debug, serde::Deserialize)]
struct AppConfig {
    database: DatabaseConfig,
    server: ServerConfig,
}

#[derive(Debug, serde::Deserialize)]
struct DatabaseConfig {
    url: String,
}

#[derive(Debug, serde::Deserialize)]
struct ServerConfig {
    host: String,
    port: u16,
}

// passing around a shared state (connection pool)
#[derive(Clone)]
struct AppState {
    client: reqwest::Client,
    pool: Pool<Postgres>,
}

// main, with routes
#[tokio::main]
async fn main() -> Result<()> {
    // init client for outgoing requests
    let client = reqwest::Client::new();

    // init DB connection
    let pool = Pool::<Postgres>::connect(&CONFIG.database.url).await?;
    println!("Connected to the PostgreSQL database.");

    // build the Axum app
    let app_state = AppState {client, pool};
    let app = Router::new()
        // routes
        .route("/", get(handlers::home_handler))
        .route("/vertices/{project_id}/{element_id}", get(handlers::get_element_vertices_handler)) // this is just to test things, at the moment
        .route("/elements/ifc_classes", get(handlers::get_available_ifc_classes))
        .route("/tilesets/projects", get(handlers::get_projects_handler))
        .route("/tilesets/{project_id}", get(handlers::get_tileset_handler))
        .route("/tilesets/models/{*gltf_path}", get(handlers::get_model_handler))
        .route("/geospatial/intersects", get(handlers::point_intersects_handler))
        .route("/geospatial/fgb/districts", get(handlers::get_districts_fgb_handler))
        .route("/geospatial/fgb/osm/buildings", get(handlers_geospatial::get_osm_buildings_handler))
        .route("/risk-scores/hazards/flood", post(handlers_hazard_scores::get_flood_hazard_batch_scores_handler))
        .route("/risk-scores/hazards/landslide", post(handlers_hazard_scores::get_landslide_hazard_batch_scores_handler))
        .route("/risk-scores/hazards/seismic", post(handlers_hazard_scores::get_seismic_hazard_batch_scores_handler))
        // adding state and CORS
        .with_state(app_state)
        .layer(
            CorsLayer::new()
                .allow_origin(Any) // OR restrict to specific domain
                .allow_headers(Any)
                .allow_methods(Any),
        );

    // running the Axum app on some address
    let server_host = &CONFIG.server.host;
    let server_port = CONFIG.server.port;
    let ip: Ipv4Addr = server_host.parse().expect("Invalid IP address");
    let addr = SocketAddr::from((ip, server_port));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    println!("Server listening on http://{}...", addr);
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
