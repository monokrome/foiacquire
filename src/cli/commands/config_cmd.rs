//! Configuration management commands.

use std::path::Path;

use console::style;
use sha2::{Digest, Sha256};

use crate::cli::icons::{error, success};
use crate::config::{AppConfigSnapshot, Config, Settings};

/// Migrate a config file into the database.
pub async fn cmd_config_transfer(settings: &Settings, file: Option<&Path>) -> anyhow::Result<()> {
    // Load config from file (explicit path or auto-discover)
    let config = if let Some(path) = file {
        if !path.exists() {
            anyhow::bail!("Config file not found: {}", path.display());
        }
        Config::load_from_path(path)
            .await
            .map_err(|e| anyhow::anyhow!(e))?
    } else {
        let loaded = Config::load().await;
        if loaded.source_path.is_none() {
            anyhow::bail!("No config file found. Use --file to specify a path.");
        }
        loaded
    };

    let source_path = config
        .source_path
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "auto-discovered".to_string());

    // Extract AppConfigSnapshot
    let snapshot = config.to_app_snapshot();

    // Serialize to JSON
    let json = serde_json::to_string_pretty(&snapshot)?;

    // Compute hash
    let mut hasher = Sha256::new();
    hasher.update(json.as_bytes());
    let hash = hex::encode(hasher.finalize());

    // Save to DB
    let ctx = settings.create_db_context()?;
    let config_repo = ctx.config_history();

    let inserted = config_repo.insert_if_new(&json, "json", &hash).await?;

    if inserted {
        eprintln!("{} Config transferred to database", success());
        eprintln!("  {} Source: {}", style("→").dim(), source_path);
        eprintln!("  {} Hash: {}", style("→").dim(), &hash[..16]);
    } else {
        eprintln!(
            "{} Config already exists in database (same hash)",
            style("!").yellow()
        );
        eprintln!("  {} Hash: {}", style("→").dim(), &hash[..16]);
    }

    Ok(())
}

/// Get a config value from the database.
pub async fn cmd_config_get(settings: &Settings, setting: &str) -> anyhow::Result<()> {
    let ctx = settings.create_db_context()?;
    let config_repo = ctx.config_history();

    // Load latest config
    let entry = config_repo.get_latest().await?.ok_or_else(|| {
        anyhow::anyhow!("No configuration found in database. Run 'config transfer' first.")
    })?;

    // Parse as JSON Value for flexible navigation
    let value: serde_json::Value = serde_json::from_str(&entry.data)?;

    // Navigate to the setting
    let result = navigate_json(&value, setting)?;

    // Print the value
    match result {
        serde_json::Value::String(s) => println!("{}", s),
        serde_json::Value::Null => println!("null"),
        other => println!("{}", serde_json::to_string_pretty(&other)?),
    }

    Ok(())
}

/// Set a config value in the database.
pub async fn cmd_config_set(settings: &Settings, setting: &str, value: &str) -> anyhow::Result<()> {
    let ctx = settings.create_db_context()?;
    let config_repo = ctx.config_history();

    // Load latest config or start with empty
    let mut json_value: serde_json::Value = match config_repo.get_latest().await? {
        Some(entry) => serde_json::from_str(&entry.data)?,
        None => serde_json::to_value(AppConfigSnapshot::default())?,
    };

    // Parse the value (try JSON first, fall back to string)
    let new_value: serde_json::Value = serde_json::from_str(value).unwrap_or_else(|_| {
        // Try as number
        if let Ok(n) = value.parse::<i64>() {
            serde_json::Value::Number(n.into())
        } else if let Ok(n) = value.parse::<f64>() {
            serde_json::Number::from_f64(n)
                .map(serde_json::Value::Number)
                .unwrap_or_else(|| serde_json::Value::String(value.to_string()))
        } else if value == "true" {
            serde_json::Value::Bool(true)
        } else if value == "false" {
            serde_json::Value::Bool(false)
        } else {
            serde_json::Value::String(value.to_string())
        }
    });

    // Set the value at the path
    set_json_value(&mut json_value, setting, new_value)?;

    // Validate by deserializing into AppConfigSnapshot
    let _snapshot: AppConfigSnapshot = serde_json::from_value(json_value.clone())
        .map_err(|e| anyhow::anyhow!("Invalid config after update: {}", e))?;

    // Serialize back to JSON
    let json_str = serde_json::to_string_pretty(&json_value)?;

    // Compute hash
    let mut hasher = Sha256::new();
    hasher.update(json_str.as_bytes());
    let hash = hex::encode(hasher.finalize());

    // Save to DB
    let inserted = config_repo.insert_if_new(&json_str, "json", &hash).await?;

    if inserted {
        eprintln!("{} Config updated", success());
        eprintln!("  {} {}: {}", style("→").dim(), setting, value);
    } else {
        eprintln!("{} No change (value already set)", style("!").yellow());
    }

    Ok(())
}

/// Navigate a JSON value by dot-separated path.
fn navigate_json<'a>(
    value: &'a serde_json::Value,
    path: &str,
) -> anyhow::Result<&'a serde_json::Value> {
    if path.is_empty() {
        return Ok(value);
    }

    let parts: Vec<&str> = path.split('.').collect();
    let mut current = value;

    for part in &parts {
        current = match current {
            serde_json::Value::Object(map) => map
                .get(*part)
                .ok_or_else(|| anyhow::anyhow!("{} Setting '{}' not found", error(), path))?,
            serde_json::Value::Array(arr) => {
                let idx: usize = part
                    .parse()
                    .map_err(|_| anyhow::anyhow!("{} Invalid array index: {}", error(), part))?;
                arr.get(idx).ok_or_else(|| {
                    anyhow::anyhow!("{} Array index out of bounds: {}", error(), idx)
                })?
            }
            _ => anyhow::bail!(
                "{} Cannot navigate into non-object/array at '{}'",
                error(),
                part
            ),
        };
    }

    Ok(current)
}

/// Set a value in a JSON object at a dot-separated path.
fn set_json_value(
    root: &mut serde_json::Value,
    path: &str,
    value: serde_json::Value,
) -> anyhow::Result<()> {
    if path.is_empty() {
        *root = value;
        return Ok(());
    }

    let parts: Vec<&str> = path.split('.').collect();
    let mut current = root;

    for (i, part) in parts.iter().enumerate() {
        let is_last = i == parts.len() - 1;

        if is_last {
            match current {
                serde_json::Value::Object(map) => {
                    map.insert(part.to_string(), value);
                    return Ok(());
                }
                _ => anyhow::bail!("Cannot set value at '{}': parent is not an object", path),
            }
        }

        // Navigate or create intermediate objects
        current = match current {
            serde_json::Value::Object(map) => {
                if !map.contains_key(*part) {
                    map.insert(part.to_string(), serde_json::json!({}));
                }
                map.get_mut(*part).unwrap()
            }
            _ => anyhow::bail!("Cannot navigate through non-object at '{}'", part),
        };
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_navigate_json_top_level() {
        let value = json!({"user_agent": "Test/1.0", "request_timeout": 60});
        let result = navigate_json(&value, "user_agent").unwrap();
        assert_eq!(result, &json!("Test/1.0"));
    }

    #[test]
    fn test_navigate_json_nested() {
        let value = json!({
            "scrapers": {
                "my-source": {
                    "name": "My Source",
                    "base_url": "https://example.com"
                }
            }
        });
        let result = navigate_json(&value, "scrapers.my-source.name").unwrap();
        assert_eq!(result, &json!("My Source"));
    }

    #[test]
    fn test_navigate_json_array_index() {
        let value = json!({
            "analysis": {
                "ocr_backends": ["tesseract", "paddleocr"]
            }
        });
        let result = navigate_json(&value, "analysis.ocr_backends.0").unwrap();
        assert_eq!(result, &json!("tesseract"));
    }

    #[test]
    fn test_navigate_json_not_found() {
        let value = json!({"user_agent": "Test/1.0"});
        let result = navigate_json(&value, "nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_navigate_json_empty_path() {
        let value = json!({"user_agent": "Test/1.0"});
        let result = navigate_json(&value, "").unwrap();
        assert_eq!(result, &value);
    }

    #[test]
    fn test_set_json_value_top_level() {
        let mut value = json!({"user_agent": "Old/1.0"});
        set_json_value(&mut value, "user_agent", json!("New/2.0")).unwrap();
        assert_eq!(value["user_agent"], json!("New/2.0"));
    }

    #[test]
    fn test_set_json_value_nested() {
        let mut value = json!({
            "scrapers": {
                "my-source": {
                    "name": "Old Name"
                }
            }
        });
        set_json_value(&mut value, "scrapers.my-source.name", json!("New Name")).unwrap();
        assert_eq!(value["scrapers"]["my-source"]["name"], json!("New Name"));
    }

    #[test]
    fn test_set_json_value_creates_intermediate() {
        let mut value = json!({});
        set_json_value(&mut value, "scrapers.new-source.name", json!("Test")).unwrap();
        assert_eq!(value["scrapers"]["new-source"]["name"], json!("Test"));
    }

    #[test]
    fn test_set_json_value_new_field() {
        let mut value = json!({"existing": "value"});
        set_json_value(&mut value, "new_field", json!(42)).unwrap();
        assert_eq!(value["new_field"], json!(42));
    }

    #[test]
    fn test_set_json_value_empty_path() {
        let mut value = json!({"old": "data"});
        set_json_value(&mut value, "", json!({"new": "data"})).unwrap();
        assert_eq!(value, json!({"new": "data"}));
    }

    #[test]
    fn test_set_json_value_complex_object() {
        let mut value = json!({});
        let scraper_config = json!({
            "name": "New Source",
            "base_url": "https://example.com",
            "rate_limit": {"requests_per_minute": 10}
        });
        set_json_value(&mut value, "scrapers.new-source", scraper_config.clone()).unwrap();
        assert_eq!(value["scrapers"]["new-source"], scraper_config);
    }
}
