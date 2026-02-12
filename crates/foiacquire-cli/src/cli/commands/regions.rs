//! Region boundary data loading command.

use console::style;
#[cfg(feature = "postgres")]
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};

use foiacquire::config::Settings;

/// Load region boundary data from GeoJSON into the regions table.
///
/// Requires PostgreSQL with PostGIS. Loads Natural Earth country boundaries
/// and optionally US state boundaries from embedded or user-provided GeoJSON.
pub async fn cmd_load_regions(settings: &Settings, file: Option<&str>) -> anyhow::Result<()> {
    let ctx = settings.create_db_context()?;
    let doc_repo = ctx.documents();

    // Check if we're on PostgreSQL with PostGIS
    let is_postgis = foiacquire::with_conn_split!(doc_repo.pool,
        sqlite: _conn => Ok::<bool, diesel::result::Error>(false),
        postgres: conn => {
            let result: Result<Vec<foiacquire::repository::diesel_document::DocIdRow>, _> =
                diesel_async::RunQueryDsl::load(
                    diesel::sql_query("SELECT PostGIS_Version() as id"),
                    &mut conn,
                )
                .await;
            Ok(result.is_ok())
        }
    )?;

    if !is_postgis {
        anyhow::bail!(
            "Region loading requires PostgreSQL with PostGIS.\n\
             Install PostGIS and run: CREATE EXTENSION IF NOT EXISTS postgis;"
        );
    }

    // Enable PostGIS if not already
    foiacquire::with_conn_split!(doc_repo.pool,
        sqlite: _conn => Ok::<_, diesel::result::Error>(()),
        postgres: conn => {

            conn.batch_execute("CREATE EXTENSION IF NOT EXISTS postgis").await?;
            Ok(())
        }
    )?;

    // Ensure regions table exists
    foiacquire::with_conn_split!(doc_repo.pool,
        sqlite: _conn => Ok::<_, diesel::result::Error>(()),
        postgres: conn => {

            conn.batch_execute(
                r#"CREATE TABLE IF NOT EXISTS regions (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    region_type TEXT NOT NULL,
                    iso_code TEXT,
                    geom GEOGRAPHY(MultiPolygon, 4326) NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_regions_name_type ON regions (name, region_type);
                CREATE INDEX IF NOT EXISTS idx_regions_geom ON regions USING GIST (geom);
                CREATE INDEX IF NOT EXISTS idx_regions_name ON regions (lower(name))"#,
            )
            .await?;
            Ok(())
        }
    )?;

    if let Some(custom_file) = file {
        let geojson_str = tokio::fs::read_to_string(custom_file).await?;
        let count = load_geojson_features(&doc_repo, &geojson_str, "custom").await?;
        println!(
            "{} Loaded {} regions from {}",
            style("✓").green(),
            count,
            custom_file
        );
        return Ok(());
    }

    // Load embedded country data
    let country_count =
        load_geojson_features(&doc_repo, foiacquire::gis_data::COUNTRIES, "country").await?;
    println!(
        "{} Loaded {} country boundaries",
        style("✓").green(),
        country_count
    );

    // Load embedded US state data
    let state_count =
        load_geojson_features(&doc_repo, foiacquire::gis_data::STATES_PROVINCES, "state").await?;
    println!(
        "{} Loaded {} state/province boundaries",
        style("✓").green(),
        state_count
    );

    println!(
        "{} Total: {} regions loaded",
        style("✓").green(),
        country_count + state_count
    );

    Ok(())
}

/// Parse GeoJSON features and upsert into the regions table.
#[allow(unused_variables)]
async fn load_geojson_features(
    doc_repo: &foiacquire::repository::DieselDocumentRepository,
    geojson_str: &str,
    region_type: &str,
) -> anyhow::Result<usize> {
    let geojson: serde_json::Value = serde_json::from_str(geojson_str)?;

    let features = geojson
        .get("features")
        .and_then(|f| f.as_array())
        .ok_or_else(|| anyhow::anyhow!("Invalid GeoJSON: missing features array"))?;

    let mut count = 0usize;

    for feature in features {
        let properties = match feature.get("properties") {
            Some(p) => p,
            None => continue,
        };
        let geometry = match feature.get("geometry") {
            Some(g) => g,
            None => continue,
        };

        // Extract name — try several common property keys
        let name = properties
            .get("NAME")
            .or_else(|| properties.get("name"))
            .or_else(|| properties.get("ADMIN"))
            .or_else(|| properties.get("admin"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if name.is_empty() {
            continue;
        }

        // Extract ISO code
        let iso_code = properties
            .get("ISO_A2")
            .or_else(|| properties.get("iso_a2"))
            .or_else(|| properties.get("iso_3166_2"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let geom_str = serde_json::to_string(geometry)?;

        // Convert geometry type if needed (Polygon -> MultiPolygon)
        let geom_type = geometry.get("type").and_then(|t| t.as_str()).unwrap_or("");

        let geom_expr = if geom_type == "Polygon" {
            "ST_Multi(ST_GeomFromGeoJSON($4))::geography"
        } else {
            "ST_GeomFromGeoJSON($4)::geography"
        };

        let result = foiacquire::with_conn_split!(doc_repo.pool,
            sqlite: _conn => Ok::<_, diesel::result::Error>(()),
            postgres: conn => {
                let insert_sql = format!(
                    "INSERT INTO regions (name, region_type, iso_code, geom) \
                     VALUES ($1, $2, $3, {}) \
                     ON CONFLICT (name, region_type) DO UPDATE \
                     SET iso_code = EXCLUDED.iso_code, geom = EXCLUDED.geom",
                    geom_expr
                );
                diesel::sql_query(&insert_sql)
                    .bind::<diesel::sql_types::Text, _>(&name)
                    .bind::<diesel::sql_types::Text, _>(region_type)
                    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(iso_code.as_deref())
                    .bind::<diesel::sql_types::Text, _>(&geom_str)
                    .execute(&mut conn)
                    .await?;
                Ok(())
            }
        );

        match result {
            Ok(()) => count += 1,
            Err(e) => {
                tracing::warn!("Failed to insert region '{}': {}", name, e);
            }
        }
    }

    Ok(count)
}
