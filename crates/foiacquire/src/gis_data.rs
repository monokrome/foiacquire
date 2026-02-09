//! Embedded GIS data for region boundary loading.

pub static COUNTRIES: &str = include_str!("../data/ne_110m_admin_0_countries.geojson");
pub static STATES_PROVINCES: &str =
    include_str!("../data/ne_110m_admin_1_states_provinces.geojson");
