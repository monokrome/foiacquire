//! Static location resolver using embedded GeoNames data.
//!
//! Resolves location names to lat/lng coordinates using the GeoNames cities15000
//! dataset (~25,000 cities with population > 15,000). Data is embedded at compile
//! time and parsed lazily on first use.
//!
//! GeoNames data is licensed under CC BY 4.0.

use std::collections::HashMap;
use std::sync::LazyLock;

static CITIES_DATA: &str = include_str!("../../data/cities15000.txt");

/// Parsed city entries: lowercase name -> (latitude, longitude).
/// For duplicate city names, the entry with the largest population wins.
static CITY_LOOKUP: LazyLock<HashMap<String, (f64, f64)>> = LazyLock::new(|| {
    let mut map: HashMap<String, (f64, f64, i64)> = HashMap::new();

    for line in CITIES_DATA.lines() {
        let fields: Vec<&str> = line.split('\t').collect();
        // GeoNames tab-separated format:
        // 0: geonameid, 1: name, 2: asciiname, 3: alternatenames,
        // 4: latitude, 5: longitude, 6: feature class, 7: feature code,
        // 8: country code, 9: cc2, 10: admin1, 11: admin2, 12: admin3, 13: admin4,
        // 14: population, 15: elevation, 16: dem, 17: timezone, 18: modification date
        if fields.len() < 15 {
            continue;
        }

        let lat: f64 = match fields[4].parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let lon: f64 = match fields[5].parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let population: i64 = fields[14].parse().unwrap_or(0);

        // Index by primary name and ASCII name
        for name_field in [fields[1], fields[2]] {
            let key = name_field.to_lowercase();
            if key.is_empty() {
                continue;
            }
            match map.get(&key) {
                Some(&(_, _, existing_pop)) if existing_pop >= population => {}
                _ => {
                    map.insert(key, (lat, lon, population));
                }
            }
        }
    }

    map.into_iter()
        .map(|(k, (lat, lon, _))| (k, (lat, lon)))
        .collect()
});

/// Notable locations relevant to FOIA/government documents that may not appear
/// in the GeoNames cities database, plus US state centroids.
static NOTABLE_LOCATIONS: LazyLock<HashMap<&'static str, (f64, f64)>> = LazyLock::new(|| {
    HashMap::from([
        // Government/intelligence facilities
        ("langley", (38.9338, -77.1771)),
        ("fort meade", (39.1087, -76.7714)),
        ("quantico", (38.5227, -77.2912)),
        ("camp david", (39.6483, -77.4649)),
        ("area 51", (37.2350, -115.8111)),
        ("guantanamo", (19.9024, -75.0961)),
        ("pentagon", (38.8711, -77.0559)),
        ("camp peary", (37.4415, -76.6791)),
        ("pine gap", (-23.7990, 133.7370)),
        ("groom lake", (37.2350, -115.8111)),
        ("washington d.c.", (38.9072, -77.0369)),
        ("washington dc", (38.9072, -77.0369)),
        // US state centroids
        ("alabama", (32.806671, -86.791130)),
        ("alaska", (61.370716, -152.404419)),
        ("arizona", (33.729759, -111.431221)),
        ("arkansas", (34.969704, -92.373123)),
        ("california", (36.116203, -119.681564)),
        ("colorado", (39.059811, -105.311104)),
        ("connecticut", (41.597782, -72.755371)),
        ("delaware", (39.318523, -75.507141)),
        ("florida", (27.766279, -81.686783)),
        ("georgia", (33.040619, -83.643074)),
        ("hawaii", (21.094318, -157.498337)),
        ("idaho", (44.240459, -114.478828)),
        ("illinois", (40.349457, -88.986137)),
        ("indiana", (39.849426, -86.258278)),
        ("iowa", (42.011539, -93.210526)),
        ("kansas", (38.526600, -96.726486)),
        ("kentucky", (37.668140, -84.670067)),
        ("louisiana", (31.169546, -91.867805)),
        ("maine", (44.693947, -69.381927)),
        ("maryland", (39.063946, -76.802101)),
        ("massachusetts", (42.230171, -71.530106)),
        ("michigan", (43.326618, -84.536095)),
        ("minnesota", (45.694454, -93.900192)),
        ("mississippi", (32.741646, -89.678696)),
        ("missouri", (38.456085, -92.288368)),
        ("montana", (46.921925, -110.454353)),
        ("nebraska", (41.125370, -98.268082)),
        ("nevada", (38.313515, -117.055374)),
        ("new hampshire", (43.452492, -71.563896)),
        ("new jersey", (40.298904, -74.521011)),
        ("new mexico", (34.840515, -106.248482)),
        ("new york", (42.165726, -74.948051)),
        ("north carolina", (35.630066, -79.806419)),
        ("north dakota", (47.528912, -99.784012)),
        ("ohio", (40.388783, -82.764915)),
        ("oklahoma", (35.565342, -96.928917)),
        ("oregon", (44.572021, -122.070938)),
        ("pennsylvania", (40.590752, -77.209755)),
        ("rhode island", (41.680893, -71.511780)),
        ("south carolina", (33.856892, -80.945007)),
        ("south dakota", (44.299782, -99.438828)),
        ("tennessee", (35.747845, -86.692345)),
        ("texas", (31.054487, -97.563461)),
        ("utah", (40.150032, -111.862434)),
        ("vermont", (44.045876, -72.710686)),
        ("virginia", (37.769337, -78.169968)),
        ("washington", (47.400902, -121.490494)),
        ("west virginia", (38.491226, -80.954456)),
        ("wisconsin", (44.268543, -89.616508)),
        ("wyoming", (42.755966, -107.302490)),
    ])
});

/// Look up coordinates for a location name.
///
/// Checks notable/government locations first, then falls back to the GeoNames
/// cities database. Returns `None` if the location is unknown.
pub fn lookup(location_name: &str) -> Option<(f64, f64)> {
    let key = location_name.to_lowercase();

    // Check notable locations first (government facilities, state centroids)
    if let Some(&coords) = NOTABLE_LOCATIONS.get(key.as_str()) {
        return Some(coords);
    }

    // Fall back to GeoNames cities
    CITY_LOOKUP.get(&key).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lookup_notable_location() {
        let result = lookup("Langley");
        assert!(result.is_some());
        let (lat, lon) = result.unwrap();
        assert!((lat - 38.9338).abs() < 0.01);
        assert!((lon - (-77.1771)).abs() < 0.01);
    }

    #[test]
    fn test_lookup_us_state() {
        let result = lookup("Virginia");
        assert!(result.is_some());
        let (lat, _lon) = result.unwrap();
        assert!(lat > 37.0 && lat < 39.0);
    }

    #[test]
    fn test_lookup_city() {
        let result = lookup("Moscow");
        assert!(result.is_some());
        let (lat, lon) = result.unwrap();
        assert!(lat > 55.0 && lat < 56.0);
        assert!(lon > 37.0 && lon < 38.0);
    }

    #[test]
    fn test_lookup_case_insensitive() {
        let r1 = lookup("LONDON");
        let r2 = lookup("london");
        let r3 = lookup("London");
        assert!(r1.is_some());
        assert_eq!(r1, r2);
        assert_eq!(r2, r3);
    }

    #[test]
    fn test_lookup_unknown() {
        let result = lookup("xyznonexistentplace");
        assert!(result.is_none());
    }

    #[test]
    fn test_lookup_washington_dc() {
        let result = lookup("Washington D.C.");
        assert!(result.is_some());
    }
}
