//! Migration Parity Tests
//!
//! Verifies that cetane migrations produce equivalent schemas to the original
//! SQLite/PostgreSQL SQL migrations.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{Connection, Result as SqliteResult};

/// Represents a SQLite table schema
#[derive(Debug, Clone, PartialEq, Eq)]
struct TableSchema {
    name: String,
    columns: BTreeMap<String, ColumnInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnInfo {
    name: String,
    col_type: String,
    not_null: bool,
    default_value: Option<String>,
    primary_key: bool,
}

/// Represents a SQLite index
#[derive(Debug, Clone, PartialEq, Eq)]
struct IndexInfo {
    name: String,
    table: String,
    columns: Vec<String>,
    unique: bool,
    partial: Option<String>,
}

/// Extract table schemas from a SQLite connection
fn extract_tables(conn: &Connection) -> SqliteResult<BTreeMap<String, TableSchema>> {
    let mut tables = BTreeMap::new();

    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;

    let table_names: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<SqliteResult<Vec<_>>>()?;

    for table_name in table_names {
        let mut columns = BTreeMap::new();

        let mut pragma = conn.prepare(&format!("PRAGMA table_info(\"{}\")", table_name))?;
        let column_iter = pragma.query_map([], |row| {
            Ok(ColumnInfo {
                name: row.get(1)?,
                col_type: row.get::<_, String>(2)?.to_uppercase(),
                not_null: row.get(3)?,
                default_value: row.get(4)?,
                primary_key: row.get::<_, i32>(5)? > 0,
            })
        })?;

        for col in column_iter {
            let col = col?;
            columns.insert(col.name.clone(), col);
        }

        tables.insert(
            table_name.clone(),
            TableSchema {
                name: table_name,
                columns,
            },
        );
    }

    Ok(tables)
}

/// Extract indexes from a SQLite connection
fn extract_indexes(conn: &Connection) -> SqliteResult<BTreeMap<String, IndexInfo>> {
    let mut indexes = BTreeMap::new();

    let mut stmt = conn.prepare(
        "SELECT name, tbl_name, sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL ORDER BY name",
    )?;

    let index_iter = stmt.query_map([], |row| {
        let name: String = row.get(0)?;
        let table: String = row.get(1)?;
        let sql: String = row.get(2)?;

        let unique = sql.to_uppercase().contains("UNIQUE");
        let partial = if sql.to_uppercase().contains(" WHERE ") {
            let idx = sql.to_uppercase().find(" WHERE ").unwrap();
            Some(sql[idx + 7..].trim().to_string())
        } else {
            None
        };

        Ok((name, table, unique, partial))
    })?;

    for result in index_iter {
        let (name, table, unique, partial) = result?;

        // Get columns for this index
        let mut pragma = conn.prepare(&format!("PRAGMA index_info(\"{}\")", name))?;
        let columns: Vec<String> = pragma
            .query_map([], |row| {
                // Column name can be NULL for expression indexes
                row.get::<_, Option<String>>(2)
                    .map(|opt| opt.unwrap_or_else(|| "<expr>".to_string()))
            })?
            .collect::<SqliteResult<Vec<_>>>()?;

        indexes.insert(
            name.clone(),
            IndexInfo {
                name,
                table,
                columns,
                unique,
                partial,
            },
        );
    }

    Ok(indexes)
}

/// Extract trigger names from a SQLite connection
fn extract_triggers(conn: &Connection) -> SqliteResult<BTreeSet<String>> {
    let mut stmt =
        conn.prepare("SELECT name FROM sqlite_master WHERE type='trigger' ORDER BY name")?;

    let triggers: BTreeSet<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<SqliteResult<BTreeSet<_>>>()?;

    Ok(triggers)
}

/// Load and run original SQL migrations
fn run_original_migrations(conn: &Connection) -> SqliteResult<()> {
    // Initial schema
    conn.execute_batch(include_str!(
        "../migrations/sqlite/2024-12-26-000000_initial_schema/up.sql"
    ))?;

    // Service status
    conn.execute_batch(include_str!(
        "../migrations/sqlite/2024-12-30-000000_service_status/up.sql"
    ))?;

    // Analysis results
    conn.execute_batch(include_str!(
        "../migrations/sqlite/2024-12-30-100000_analysis_results/up.sql"
    ))?;

    // Unique constraints
    conn.execute_batch(include_str!(
        "../migrations/sqlite/2025-01-01-000000_add_missing_unique_constraints/up.sql"
    ))?;

    // Schema drift fix (SQLite-only)
    conn.execute_batch(include_str!(
        "../migrations/sqlite/2025-01-01-100000_fix_schema_drift/up.sql"
    ))?;

    // Archive history
    conn.execute_batch(include_str!(
        "../migrations/sqlite/2025-01-01-200000_archive_history/up.sql"
    ))?;

    // Page OCR results
    conn.execute_batch(include_str!(
        "../migrations/sqlite/2025-01-22-000000_page_ocr_results/up.sql"
    ))?;

    // Add model column
    conn.execute_batch(include_str!(
        "../migrations/sqlite/2025-01-22-100000_add_model_column/up.sql"
    ))?;

    // Page image hash
    conn.execute_batch(include_str!(
        "../migrations/sqlite/2025-01-24-000000_page_image_hash/up.sql"
    ))?;

    Ok(())
}

/// Run cetane migrations (generates SQL for SQLite backend)
fn run_cetane_migrations(conn: &Connection) -> SqliteResult<()> {
    use cetane::backend::Sqlite;

    let registry = foiacquire::migrations::registry();
    let backend = Sqlite;

    // Get migration names in dependency order
    let ordered_names = registry
        .resolve_order()
        .expect("Failed to resolve migration order");

    for name in ordered_names {
        let migration = registry
            .get(name)
            .expect("Migration not found after resolve");
        let statements = migration.forward_sql(&backend);
        for stmt in statements {
            if stmt.trim().is_empty() {
                continue;
            }
            conn.execute_batch(&stmt)?;
        }
    }

    Ok(())
}

/// Normalize type names for comparison (SQLite is flexible with types)
fn normalize_type(t: &str) -> String {
    let t = t.to_uppercase();
    // SQLite type affinity rules
    if t.contains("INT") {
        return "INTEGER".to_string();
    }
    if t.contains("CHAR") || t.contains("CLOB") || t.contains("TEXT") {
        return "TEXT".to_string();
    }
    if t.contains("BLOB") {
        return "BLOB".to_string();
    }
    if t.contains("REAL") || t.contains("FLOA") || t.contains("DOUB") {
        return "REAL".to_string();
    }
    t
}

/// Compare two schemas and return differences
fn compare_schemas(
    original: &BTreeMap<String, TableSchema>,
    cetane: &BTreeMap<String, TableSchema>,
) -> Vec<String> {
    let mut diffs = Vec::new();

    // Check for missing tables
    for name in original.keys() {
        if !cetane.contains_key(name) {
            diffs.push(format!("Missing table in cetane: {}", name));
        }
    }
    for name in cetane.keys() {
        if !original.contains_key(name) {
            diffs.push(format!("Extra table in cetane: {}", name));
        }
    }

    // Compare columns in shared tables
    for (name, orig_table) in original {
        if let Some(cetane_table) = cetane.get(name) {
            for (col_name, orig_col) in &orig_table.columns {
                if let Some(cetane_col) = cetane_table.columns.get(col_name) {
                    // Compare types (with normalization)
                    let orig_type = normalize_type(&orig_col.col_type);
                    let cetane_type = normalize_type(&cetane_col.col_type);
                    if orig_type != cetane_type {
                        diffs.push(format!(
                            "Type mismatch in {}.{}: original={}, cetane={}",
                            name, col_name, orig_col.col_type, cetane_col.col_type
                        ));
                    }

                    // Compare NOT NULL
                    if orig_col.not_null != cetane_col.not_null {
                        diffs.push(format!(
                            "NOT NULL mismatch in {}.{}: original={}, cetane={}",
                            name, col_name, orig_col.not_null, cetane_col.not_null
                        ));
                    }

                    // Compare primary key
                    if orig_col.primary_key != cetane_col.primary_key {
                        diffs.push(format!(
                            "PRIMARY KEY mismatch in {}.{}: original={}, cetane={}",
                            name, col_name, orig_col.primary_key, cetane_col.primary_key
                        ));
                    }
                } else {
                    diffs.push(format!("Missing column in cetane: {}.{}", name, col_name));
                }
            }

            for col_name in cetane_table.columns.keys() {
                if !orig_table.columns.contains_key(col_name) {
                    diffs.push(format!("Extra column in cetane: {}.{}", name, col_name));
                }
            }
        }
    }

    diffs
}

/// Compare indexes between original and cetane schemas
fn compare_indexes(
    original: &BTreeMap<String, IndexInfo>,
    cetane: &BTreeMap<String, IndexInfo>,
) -> Vec<String> {
    let mut diffs = Vec::new();

    // Build a set of (table, columns, unique) tuples for semantic comparison
    // Index names may differ but the actual index should be equivalent
    let orig_semantic: BTreeSet<_> = original
        .values()
        .map(|idx| (&idx.table, &idx.columns, idx.unique))
        .collect();

    let cetane_semantic: BTreeSet<_> = cetane
        .values()
        .map(|idx| (&idx.table, &idx.columns, idx.unique))
        .collect();

    for (table, cols, unique) in &orig_semantic {
        if !cetane_semantic.contains(&(*table, *cols, *unique)) {
            diffs.push(format!(
                "Missing index in cetane: table={}, columns={:?}, unique={}",
                table, cols, unique
            ));
        }
    }

    for (table, cols, unique) in &cetane_semantic {
        if !orig_semantic.contains(&(*table, *cols, *unique)) {
            diffs.push(format!(
                "Extra index in cetane: table={}, columns={:?}, unique={}",
                table, cols, unique
            ));
        }
    }

    diffs
}

#[test]
fn test_schema_parity() {
    // Create two in-memory SQLite databases
    let orig_conn = Connection::open_in_memory().expect("Failed to open original DB");
    let cetane_conn = Connection::open_in_memory().expect("Failed to open cetane DB");

    // Run migrations
    run_original_migrations(&orig_conn).expect("Failed to run original migrations");
    run_cetane_migrations(&cetane_conn).expect("Failed to run cetane migrations");

    // Extract and compare tables
    let orig_tables = extract_tables(&orig_conn).expect("Failed to extract original tables");
    let cetane_tables = extract_tables(&cetane_conn).expect("Failed to extract cetane tables");

    let table_diffs = compare_schemas(&orig_tables, &cetane_tables);
    if !table_diffs.is_empty() {
        eprintln!("Table differences:");
        for diff in &table_diffs {
            eprintln!("  - {}", diff);
        }
    }

    // Extract and compare indexes
    let orig_indexes = extract_indexes(&orig_conn).expect("Failed to extract original indexes");
    let cetane_indexes = extract_indexes(&cetane_conn).expect("Failed to extract cetane indexes");

    let index_diffs = compare_indexes(&orig_indexes, &cetane_indexes);
    if !index_diffs.is_empty() {
        eprintln!("Index differences:");
        for diff in &index_diffs {
            eprintln!("  - {}", diff);
        }
    }

    // Extract and compare triggers
    let orig_triggers = extract_triggers(&orig_conn).expect("Failed to extract original triggers");
    let cetane_triggers =
        extract_triggers(&cetane_conn).expect("Failed to extract cetane triggers");

    if orig_triggers != cetane_triggers {
        eprintln!("Trigger differences:");
        for t in orig_triggers.difference(&cetane_triggers) {
            eprintln!("  - Missing in cetane: {}", t);
        }
        for t in cetane_triggers.difference(&orig_triggers) {
            eprintln!("  - Extra in cetane: {}", t);
        }
    }

    // Report summary
    let total_diffs = table_diffs.len() + index_diffs.len();
    if total_diffs > 0 {
        panic!("Schema parity test failed with {} differences", total_diffs);
    }

    println!("Schema parity test passed!");
    println!("  Tables: {}", orig_tables.len());
    println!("  Indexes: {}", orig_indexes.len());
    println!("  Triggers: {}", orig_triggers.len());
}

#[test]
fn test_individual_migrations_generate_valid_sql() {
    use cetane::backend::Sqlite;

    let registry = foiacquire::migrations::registry();
    let backend = Sqlite;

    let ordered_names = registry
        .resolve_order()
        .expect("Failed to resolve migration order");

    // For each migration, run all preceding migrations in order
    for (i, name) in ordered_names.iter().enumerate() {
        let conn = Connection::open_in_memory().expect("Failed to open DB");

        // Run all migrations up to and including the current one
        for prior_name in &ordered_names[..=i] {
            let migration = registry.get(prior_name).expect("Migration not found");
            let statements = migration.forward_sql(&backend);
            for stmt in &statements {
                if stmt.trim().is_empty() {
                    continue;
                }
                conn.execute_batch(stmt).unwrap_or_else(|e| {
                    panic!("Migration {} failed: {}\nSQL: {}", migration.name, e, stmt)
                });
            }
        }

        let migration = registry.get(name).expect("Migration not found");
        let statements = migration.forward_sql(&backend);
        println!(
            "Migration {} generates valid SQL ({} statements)",
            migration.name,
            statements.len()
        );
    }
}

#[test]
fn test_postgres_sql_generation() {
    use cetane::backend::Postgres;

    let registry = foiacquire::migrations::registry();
    let backend = Postgres;

    let ordered_names = registry
        .resolve_order()
        .expect("Failed to resolve migration order");

    for name in ordered_names {
        let migration = registry.get(name).expect("Migration not found");
        let statements = migration.forward_sql(&backend);

        // Just verify SQL is generated (can't run without a real Postgres)
        assert!(
            !statements.is_empty() || migration.name.contains("drift"),
            "Migration {} produced no SQL for Postgres",
            migration.name
        );

        // Check for common SQL generation issues
        for stmt in &statements {
            // Should use SERIAL for auto-increment
            if stmt.contains("AUTOINCREMENT") {
                panic!(
                    "Migration {} uses AUTOINCREMENT in Postgres SQL (should be SERIAL)",
                    migration.name
                );
            }
        }

        println!(
            "Migration {} generates {} Postgres statements",
            migration.name,
            statements.len()
        );
    }
}
