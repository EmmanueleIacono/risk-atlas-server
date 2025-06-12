use std::{
    collections::{
        BTreeSet,
        HashMap
    },
    io::Cursor
};

use anyhow::Result;
use flatgeobuf::{
    ColumnType,
    FgbWriter,
    FgbWriterOptions,
    GeometryType
};
use geo::{
    algorithm::contains::Contains,
    Point,
    Polygon
};
use geojson::{
    feature::Id,
    Feature,
    FeatureCollection,
    Geometry,
    Value as GeoValue
};
use geozero::{
    geojson::GeoJsonReader,
    ColumnValue,
    FeatureProcessor,
    GeomProcessor,
    GeozeroDatasource,
    PropertyProcessor
};
use serde_json::Value;

// HELPERS STRUCTS

struct RemappingWriter<'a> {
    /// The real writer that knows how to build FGB
    inner: FgbWriter<'a>,
    /// Map a property name -> stable column index
    column_map: &'a HashMap<String, u32>,
}

// HELPERS STRUCT IMPLS

impl<'a> RemappingWriter<'a> {
    fn new(
        all_keys: &'a BTreeSet<String>,
        column_map: &'a HashMap<String, u32>,
    ) -> Result<Self> {
        // create the FgbWriter
        let mut w = FgbWriter::create_with_options(
            "buildings",
            GeometryType::MultiPolygon, // OSM building footprints may be Polygons or MultiPolygons -> but forcing MultiPolygon
            FgbWriterOptions {
                write_index: true, // writing spatial index? YES
                ..Default::default()
            },
        )?;

        // declare every column in sorted order
        for key in all_keys {
            w.add_column(
                key,
                ColumnType::String,
                |_fbb, col| col.nullable = true,
            );
        }

        Ok(Self {
            inner: w,
            column_map,
        })
    }
}

// whenever GeoJsonReader calls "property(i, name, value)", ignore "i"
// and instead look up "name" in column_map to find the "true" index
impl<'a> PropertyProcessor for RemappingWriter<'a> {
    fn property(
        &mut self,
        _i: usize,
        colname: &str,
        colval: &ColumnValue
    ) -> geozero::error::Result<bool> {
        if let Some(&correct_idx) = self.column_map.get(colname) {
            // write the value into the correct column
            self.inner.property(correct_idx as usize, colname, colval)?;
        }
        // if the feature has a tag not in column_map, ignore it
        Ok(false)
    }
}

// delegate the rest of the trait methods to inner
impl<'a> GeomProcessor for RemappingWriter<'a> {
    fn dimensions(&self) -> geozero::CoordDimensions {
        self.inner.dimensions()
    }
    fn multi_dim(&self) -> bool {
        self.inner.multi_dim()
    }
    fn srid(&mut self, srid: Option<i32>) -> geozero::error::Result<()> {
        self.inner.srid(srid)
    }
    fn xy(&mut self, x: f64, y: f64, idx: usize) -> geozero::error::Result<()> {
        self.inner.xy(x, y, idx)
    }
    fn coordinate(
        &mut self, x: f64, y: f64, z: Option<f64>,
        m: Option<f64>, t: Option<f64>,
        tm: Option<u64>, idx: usize
    ) -> geozero::error::Result<()> {
        self.inner.coordinate(x, y, z, m, t, tm, idx)
    }
    fn empty_point(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.inner.empty_point(idx)
    }
    fn point_begin(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.inner.point_begin(idx)
    }
    fn point_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.inner.point_end(idx)
    }
    fn multipoint_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.inner.multipoint_begin(size, idx)
    }
    fn multipoint_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.inner.multipoint_end(idx)
    }
    fn linestring_begin(&mut self, tagged: bool, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.inner.linestring_begin(tagged, size, idx)
    }
    fn linestring_end(&mut self, tagged: bool, idx: usize) -> geozero::error::Result<()> {
        self.inner.linestring_end(tagged, idx)
    }
    fn multilinestring_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.inner.multilinestring_begin(size, idx)
    }
    fn multilinestring_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.inner.multilinestring_end(idx)
    }
    fn polygon_begin(&mut self, tagged: bool, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.inner.polygon_begin(tagged, size, idx)
    }
    fn polygon_end(&mut self, tagged: bool, idx: usize) -> geozero::error::Result<()> {
        self.inner.polygon_end(tagged, idx)
    }
    fn multipolygon_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.inner.multipolygon_begin(size, idx)
    }
    fn multipolygon_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.inner.multipolygon_end(idx)
    }
    fn geometrycollection_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.inner.geometrycollection_begin(size, idx)
    }
    fn geometrycollection_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.inner.geometrycollection_end(idx)
    }
    fn circularstring_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.inner.circularstring_begin(size, idx)
    }
    fn circularstring_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.inner.circularstring_end(idx)
    }
    fn compoundcurve_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.inner.compoundcurve_begin(size, idx)
    }
    fn compoundcurve_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.inner.compoundcurve_end(idx)
    }
    fn curvepolygon_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.inner.curvepolygon_begin(size, idx)
    }
    fn curvepolygon_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.inner.curvepolygon_end(idx)
    }
    fn multicurve_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.inner.multicurve_begin(size, idx)
    }
    fn multicurve_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.inner.multicurve_end(idx)
    }
    fn multisurface_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.inner.multisurface_begin(size, idx)
    }
    fn multisurface_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.inner.multisurface_end(idx)
    }
    fn triangle_begin(&mut self, tagged: bool, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.inner.triangle_begin(tagged, size, idx)
    }
    fn triangle_end(&mut self, tagged: bool, idx: usize) -> geozero::error::Result<()> {
        self.inner.triangle_end(tagged, idx)
    }
    fn polyhedralsurface_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.inner.polyhedralsurface_begin(size, idx)
    }
    fn polyhedralsurface_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.inner.polyhedralsurface_end(idx)
    }
    fn tin_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.inner.tin_begin(size, idx)
    }
    fn tin_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.inner.tin_end(idx)
    }
}

// impl FeatureProcessor as a no-op wrapper
// so GeoJsonReader can call "dataset_begin", "feature_begin", "feature_end", etc.
impl<'a> FeatureProcessor for RemappingWriter<'a> {
    fn dataset_begin(&mut self, _name: Option<&str>) -> geozero::error::Result<()> {
        self.inner.dataset_begin(_name)
    }
    fn dataset_end(&mut self) -> geozero::error::Result<()> {
        self.inner.dataset_end()
    }
    fn feature_begin(&mut self, idx: u64) -> geozero::error::Result<()> {
        self.inner.feature_begin(idx)
    }
    fn feature_end(&mut self, idx: u64) -> geozero::error::Result<()> {
        self.inner.feature_end(idx)
    }
    fn properties_begin(&mut self) -> geozero::error::Result<()> {
        self.inner.properties_begin()
    }
    fn properties_end(&mut self) -> geozero::error::Result<()> {
        self.inner.properties_end()
    }
    fn geometry_begin(&mut self) -> geozero::error::Result<()> {
        self.inner.geometry_begin()
    }
    fn geometry_end(&mut self) -> geozero::error::Result<()> {
        self.inner.geometry_end()
    }
}

// HELPERS

pub fn osm_to_geojson(
    osm_json: &Value,
) -> FeatureCollection {
    let mut features = Vec::new();
    if let Some(elements) = osm_json
        .get("elements")
        .and_then(|e| e.as_array()) {
            for el in elements {
                match el.get("type").and_then(|t| t.as_str()) {
                    // OSM "way" -> Polygons
                    Some("way") => {
                        let tags = el.get("tags").and_then(|t| t.as_object()).unwrap();
                        let id = el.get("id").and_then(|v| v.as_number()).unwrap().clone();
                        let coords = el.get("geometry").and_then(|g| g.as_array()).unwrap();

                        let ring = coords_to_ring(coords);
                        let geom = Geometry::new(GeoValue::MultiPolygon(vec![vec![ring]])); // always emitting MultiPolygon
                        let mut feat = Feature {
                            geometry: Some(geom),
                            properties: None,
                            id: Some(Id::Number(id.clone())),
                            bbox: None,
                            foreign_members: None,
                        };

                        // carrying over the tags
                        let mut props = tags.clone();
                        props.insert(
                            "osm_id".to_string(),
                            Value::String(id.to_string())
                        );
                        feat.properties = Some(props);

                        features.push(feat);
                    }

                    // OSM "relations" -> MultiPolygons
                    Some("relation") => {
                        let tags = match el.get("tags").and_then(|t| t.as_object()) {
                            Some(t) if t.get("type").and_then(|v| v.as_str()) == Some("multipolygon") => t,
                            _ => continue,
                        };

                        let id = el.get("id").and_then(|v| v.as_number()).unwrap().clone();

                        let mut outer_rings = Vec::new();
                        let mut inner_rings = Vec::new();

                        if let Some(members) = el.get("members").and_then(|m| m.as_array()) {
                            for member in members {
                                if member.get("type").and_then(|t| t.as_str()) != Some("way") {
                                    continue;
                                }
                                let role = member.get("role").and_then(|r| r.as_str()).unwrap_or("");
                                let geom = member.get("geometry").and_then(|g| g.as_array()).unwrap();
                                let ring = coords_to_ring(geom);

                                // classifying roles (treating "outline" as outer, "part" as inner)
                                if role.starts_with("outer") || role == "outline" {
                                    outer_rings.push(ring);
                                } else {
                                    inner_rings.push(ring);
                                }
                            }
                        }

                        // building GeoJSON Value
                        let gj_value = if outer_rings.len() == 1 {
                            // single Polygon -> but wrapping all in a 1-entry MultiPolygon
                            let mut rings = Vec::new();
                            rings.push(outer_rings.into_iter().next().unwrap());
                            rings.extend(inner_rings.into_iter());
                            GeoValue::MultiPolygon(vec![rings])
                        } else {
                            // multiple exteriors -> MultiPolygon
                            // grouping holes by which exterior contains them
                            let mut multipolys = Vec::new();
                            // turning each outer into a geo::Polygon for spatial testing
                            let geo_exteriors: Vec<Polygon<f64>> = outer_rings
                                .iter()
                                .map(|r| {
                                    Polygon::new(
                                        r
                                            .iter()
                                            .map(|c| (c[0], c[1]))
                                            .collect::<Vec<_>>()
                                            .into(),
                                        Vec::new(),
                                    )
                                })
                                .collect();

                            // preparing empty Vec of Vecs to collect holes per exterior
                            let mut holes_for = vec![
                                Vec::new();
                                geo_exteriors.len()
                            ];

                            for hole in inner_rings {
                                // picking a test point for the hole ring
                                let test_pt = Point::new(hole[0][0], hole[0][1]);
                                // finding which exterior contains the hole
                                for (i, poly) in geo_exteriors.iter().enumerate() {
                                    if poly.contains(&test_pt) {
                                        holes_for[i].push(hole.clone());
                                        break;
                                    }
                                }
                            }

                            // emitting each polygon as [exterior, hole_1, hole_2, etc.]
                            for (i, ext_ring) in outer_rings.into_iter().enumerate() {
                                let mut rings = Vec::new();
                                rings.push(ext_ring);
                                rings.extend(holes_for[i].drain(..));
                                multipolys.push(rings);
                            }

                            GeoValue::MultiPolygon(multipolys)
                        };

                        let geom = Geometry::new(gj_value);
                        let mut feat = Feature {
                            geometry: Some(geom),
                            properties: None,
                            id: Some(Id::Number(id.clone())),
                            bbox: None,
                            foreign_members: None,
                        };

                        // carrying over the tags
                        let mut props = tags.clone();
                        props.insert(
                            "osm_id".to_string(),
                            Value::String(id.to_string())
                        );
                        feat.properties = Some(props);

                        features.push(feat);
                    }

                    _ => {}
                }
            }
        }

        FeatureCollection {
            features,
            bbox: None,
            foreign_members: None,
        }
}

pub fn geojson_to_flatgeobuf(
    fc: &FeatureCollection,
) -> Result<Vec<u8>> {
    // 1.a building the union of all tag keys
    let mut all_keys: BTreeSet<String> = BTreeSet::new();
    for feat in &fc.features {
        if let Some(props) = &feat.properties {
            for key in props.keys() {
                all_keys.insert(key.clone());
            }
        }
    }

    // 1.b building a name->column_index map (in sorted order)
    // so that "the key at position N in the sorted set" is column N
    let mut column_map: HashMap<String, u32> = HashMap::new();
    for (idx, key) in all_keys.iter().enumerate() {
        column_map.insert(key.clone(), idx as u32);
    }

    // 2. creating the fgb writer wrapper
    let mut writer = RemappingWriter::new(&all_keys, &column_map)?;

    // 3. feeding the geojson to the writer
    let geojson_string = serde_json::to_string(fc)?;
    let mut cursor = Cursor::new(geojson_string.as_bytes());
    let mut reader = GeoJsonReader(&mut cursor);
    // at this point every feature is slotted into the fixed schema
    reader.process(&mut writer)?; // each property(name) -> correct "column_map[name]"

    // 4. serializing the flatgeobuf buffer and returning
    let mut buf = Vec::new();
    writer.inner.write(&mut buf)?;

    Ok(buf)
}

// helper to turn a member's "geometry" array into a Vec<Vec<f64>>
fn coords_to_ring(arr: &[Value]) -> Vec<Vec<f64>> {
    arr.iter()
        .map(|pt| {
            vec![
                pt.get("lon").and_then(Value::as_f64).unwrap_or_default(),
                pt.get("lat").and_then(Value::as_f64).unwrap_or_default(),
            ]
        })
        .collect()
}
