use serde::{Deserialize, Serialize};

// STRUCTS

// Input point from client
#[derive(Debug, Serialize, Deserialize)]
pub struct HazardPoint {
    pub id: String,
    pub lon: f64,
    pub lat: f64,
}

// Output score per point
#[derive(Debug, Serialize)]
pub struct HazardScore {
    pub id: String,
    pub score: f64,
}
