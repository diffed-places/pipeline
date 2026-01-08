use deepsize::DeepSizeOf;
use geo_types::Point;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

mod writer;
pub use writer::ParquetWriter;

#[derive(Debug, DeepSizeOf, Eq, PartialEq, Serialize, Deserialize)]
pub struct Place {
    pub s2_cell_id: u64,
    pub lon_e7: i32,
    pub lat_e7: i32,
    pub source_id: String,
    pub source_url: Option<String>,
    pub tags: Vec<(String, String)>,
}

impl Place {
    pub fn new(
        point: &Point,
        source_id: String,
        source_url: Option<String>,
        tags: Vec<(String, String)>,
    ) -> Option<Place> {
        let s2_lat_lng = s2::latlng::LatLng::from_degrees(point.y(), point.x());
        if !s2_lat_lng.is_valid() {
            return None;
        }

        let s2_cell_id = s2::cellid::CellID::from(s2_lat_lng).0;
        let lon_e7 = (point.x() * 1e7) as i32;
        let lat_e7 = (point.y() * 1e7) as i32;
        Some(Place {
            s2_cell_id,
            lon_e7,
            lat_e7,
            source_id,
            source_url,
            tags,
        })
    }
}

impl Ord for Place {
    fn cmp(&self, other: &Self) -> Ordering {
        // Because s2_cell_id is a projection of the geographic longitude
        // and latitude at centimeter resolution, we do not need to compare
        // lng_e7 and lat_e7.
        self.s2_cell_id
            .cmp(&other.s2_cell_id)
            .then(self.source_id.cmp(&other.source_id))
            .then(self.source_url.cmp(&other.source_url))
            .then(self.tags.cmp(&other.tags))
    }
}

impl PartialOrd for Place {
    fn partial_cmp(&self, other: &Place) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::Place;
    use geo::Point;

    #[test]
    fn test_new() {
        let p = Point::new(7.447_812_3, 46.947_980_1);
        let source_id = "test/source_id".to_string();
        let source_url = Some("https://example.org/zytglogge".to_string());
        let tags = vec![
            ("building".to_string(), "tower".to_string()),
            ("name:gsw".to_string(), "Zytglogge".to_string()),
        ];
        let place = Place::new(&p, source_id, source_url, tags.clone()).unwrap();
        let place_source_url = place.source_url.unwrap();
        assert_eq!(place.s2_cell_id, 5156122125915201443);
        assert_eq!(place.lon_e7, 7_447_812_3);
        assert_eq!(place.lat_e7, 46_947_980_1);
        assert_eq!(place.source_id, "test/source_id");
        assert_eq!(place_source_url, "https://example.org/zytglogge");
        assert_eq!(place.tags, tags);
    }

    #[test]
    fn test_cmp() {
        let a = Place::new(
            &Point::new(7.4478123, 46.9479801),
            "test/source_id".to_string(),
            None,
            vec![],
        )
        .unwrap();
        let b = Place::new(
            &Point::new(-122.4630042, 37.8045878),
            "test/source_id".to_string(),
            None,
            vec![],
        )
        .unwrap();
        assert_eq!(a.eq(&b), false);
        assert_eq!(a.eq(&a), true);
        assert_eq!(a.cmp(&b), a.s2_cell_id.cmp(&b.s2_cell_id));
        assert_eq!(a.partial_cmp(&b), a.s2_cell_id.partial_cmp(&b.s2_cell_id));
    }
}
