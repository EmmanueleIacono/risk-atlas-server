use serde::Deserialize;

// STRUCTS

#[derive(Debug, Deserialize)]
pub struct IntersectQuery {
    pub lat: f64, // latitude (Y)
    pub lon: f64, // longitude (X)
    pub epsg: i32, // SRID
}

#[derive(Deserialize)]
pub struct BBoxQuery {
    pub bbox: String, // "minLon,minLat,maxLon,maxLat" -> e.g. "7.2,44.9,7.8,45.2"
    #[serde(default = "default_epsg")]
    pub epsg: i32, // SRID
}

// DEFAULTS

fn default_epsg() -> i32 { 4326 }
