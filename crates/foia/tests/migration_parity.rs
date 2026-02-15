//! Migration Schema Tests
//!
//! Verifies that cetane migrations produce the expected database schema by
//! comparing against a checked-in schema snapshot. If a migration changes the
//! schema intentionally, regenerate the snapshot:
//!
//!   cargo test --test migration_parity regenerate_schema_snapshot -- --ignored

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{Connection, Result as SqliteResult};
use serde::{Deserialize, Serialize};

const SCHEMA_SNAPSHOT_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/expected_schema.json"
);

/// Represents a SQLite table schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TableSchema {
    name: String,
    columns: BTreeMap<String, ColumnInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ColumnInfo {
    name: String,
    col_type: String,
    not_null: bool,
    default_value: Option<String>,
    primary_key: bool,
}

/// Represents a SQLite index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct IndexInfo {
    name: String,
    table: String,
    columns: Vec<String>,
    unique: bool,
    partial: Option<String>,
}

/// Full schema snapshot for comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SchemaSnapshot {
    tables: BTreeMap<String, TableSchema>,
    indexes: BTreeMap<String, IndexInfo>,
    triggers: BTreeSet<String>,
}

/// Extract table schemas from a SQLite connection.
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

/// Extract indexes from a SQLite connection.
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

/// Extract trigger names from a SQLite connection.
fn extract_triggers(conn: &Connection) -> SqliteResult<BTreeSet<String>> {
    let mut stmt =
        conn.prepare("SELECT name FROM sqlite_master WHERE type='trigger' ORDER BY name")?;

    let triggers: BTreeSet<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<SqliteResult<BTreeSet<_>>>()?;

    Ok(triggers)
}

/// Run cetane migrations on a SQLite connection.
fn run_cetane_migrations(conn: &Connection) -> SqliteResult<()> {
    use cetane::backend::Sqlite;

    let registry = foia::migrations::registry();
    let backend = Sqlite;

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

/// Extract a full schema snapshot from a SQLite connection.
fn extract_snapshot(conn: &Connection) -> SchemaSnapshot {
    SchemaSnapshot {
        tables: extract_tables(conn).expect("Failed to extract tables"),
        indexes: extract_indexes(conn).expect("Failed to extract indexes"),
        triggers: extract_triggers(conn).expect("Failed to extract triggers"),
    }
}

/// Normalize type names for comparison (SQLite is flexible with types).
fn normalize_type(t: &str) -> String {
    let t = t.to_uppercase();
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

/// Compare two schemas and return differences.
fn compare_schemas(
    expected: &BTreeMap<String, TableSchema>,
    actual: &BTreeMap<String, TableSchema>,
) -> Vec<String> {
    let mut diffs = Vec::new();

    for name in expected.keys() {
        if !actual.contains_key(name) {
            diffs.push(format!("Missing table: {}", name));
        }
    }
    for name in actual.keys() {
        if !expected.contains_key(name) {
            diffs.push(format!("Unexpected table: {}", name));
        }
    }

    for (name, expected_table) in expected {
        if let Some(actual_table) = actual.get(name) {
            for (col_name, expected_col) in &expected_table.columns {
                if let Some(actual_col) = actual_table.columns.get(col_name) {
                    let expected_type = normalize_type(&expected_col.col_type);
                    let actual_type = normalize_type(&actual_col.col_type);
                    if expected_type != actual_type {
                        diffs.push(format!(
                            "Type mismatch in {}.{}: expected={}, actual={}",
                            name, col_name, expected_col.col_type, actual_col.col_type
                        ));
                    }

                    if expected_col.not_null != actual_col.not_null {
                        diffs.push(format!(
                            "NOT NULL mismatch in {}.{}: expected={}, actual={}",
                            name, col_name, expected_col.not_null, actual_col.not_null
                        ));
                    }

                    if expected_col.primary_key != actual_col.primary_key {
                        diffs.push(format!(
                            "PRIMARY KEY mismatch in {}.{}: expected={}, actual={}",
                            name, col_name, expected_col.primary_key, actual_col.primary_key
                        ));
                    }
                } else {
                    diffs.push(format!("Missing column: {}.{}", name, col_name));
                }
            }

            for col_name in actual_table.columns.keys() {
                if !expected_table.columns.contains_key(col_name) {
                    diffs.push(format!("Unexpected column: {}.{}", name, col_name));
                }
            }
        }
    }

    diffs
}

/// Compare indexes between expected and actual schemas.
fn compare_indexes(
    expected: &BTreeMap<String, IndexInfo>,
    actual: &BTreeMap<String, IndexInfo>,
) -> Vec<String> {
    let mut diffs = Vec::new();

    let expected_semantic: BTreeSet<_> = expected
        .values()
        .map(|idx| (&idx.table, &idx.columns, idx.unique))
        .collect();

    let actual_semantic: BTreeSet<_> = actual
        .values()
        .map(|idx| (&idx.table, &idx.columns, idx.unique))
        .collect();

    for (table, cols, unique) in &expected_semantic {
        if !actual_semantic.contains(&(*table, *cols, *unique)) {
            diffs.push(format!(
                "Missing index: table={}, columns={:?}, unique={}",
                table, cols, unique
            ));
        }
    }

    for (table, cols, unique) in &actual_semantic {
        if !expected_semantic.contains(&(*table, *cols, *unique)) {
            diffs.push(format!(
                "Unexpected index: table={}, columns={:?}, unique={}",
                table, cols, unique
            ));
        }
    }

    diffs
}

#[test]
fn test_schema_parity() {
    let snapshot_json = std::fs::read_to_string(SCHEMA_SNAPSHOT_PATH).unwrap_or_else(|_| {
        panic!(
            "Schema snapshot not found at {}.\n\
             Generate it with: cargo test --test migration_parity regenerate_schema_snapshot -- --ignored",
            SCHEMA_SNAPSHOT_PATH
        )
    });

    let expected: SchemaSnapshot =
        serde_json::from_str(&snapshot_json).expect("Failed to parse schema snapshot");

    let conn = Connection::open_in_memory().expect("Failed to open DB");
    run_cetane_migrations(&conn).expect("Failed to run cetane migrations");
    let actual = extract_snapshot(&conn);

    let table_diffs = compare_schemas(&expected.tables, &actual.tables);
    if !table_diffs.is_empty() {
        eprintln!("Table differences:");
        for diff in &table_diffs {
            eprintln!("  - {}", diff);
        }
    }

    let index_diffs = compare_indexes(&expected.indexes, &actual.indexes);
    if !index_diffs.is_empty() {
        eprintln!("Index differences:");
        for diff in &index_diffs {
            eprintln!("  - {}", diff);
        }
    }

    if expected.triggers != actual.triggers {
        eprintln!("Trigger differences:");
        for t in expected.triggers.difference(&actual.triggers) {
            eprintln!("  - Missing: {}", t);
        }
        for t in actual.triggers.difference(&expected.triggers) {
            eprintln!("  - Unexpected: {}", t);
        }
    }

    let total_diffs = table_diffs.len()
        + index_diffs.len()
        + expected
            .triggers
            .symmetric_difference(&actual.triggers)
            .count();

    if total_diffs > 0 {
        panic!(
            "Schema parity test failed with {} differences.\n\
             If these changes are intentional, regenerate the snapshot:\n  \
             cargo test --test migration_parity regenerate_schema_snapshot -- --ignored",
            total_diffs
        );
    }

    println!("Schema parity test passed!");
    println!("  Tables: {}", actual.tables.len());
    println!("  Indexes: {}", actual.indexes.len());
    println!("  Triggers: {}", actual.triggers.len());
}

#[test]
fn test_individual_migrations_generate_valid_sql() {
    use cetane::backend::Sqlite;

    let registry = foia::migrations::registry();
    let backend = Sqlite;

    let ordered_names = registry
        .resolve_order()
        .expect("Failed to resolve migration order");

    for (i, name) in ordered_names.iter().enumerate() {
        let conn = Connection::open_in_memory().expect("Failed to open DB");

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

    let registry = foia::migrations::registry();
    let backend = Postgres;

    let ordered_names = registry
        .resolve_order()
        .expect("Failed to resolve migration order");

    for name in ordered_names {
        let migration = registry.get(name).expect("Migration not found");
        let statements = migration.forward_sql(&backend);

        assert!(
            !statements.is_empty() || migration.name.contains("drift"),
            "Migration {} produced no SQL for Postgres",
            migration.name
        );

        for stmt in &statements {
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

/// Regenerate the schema snapshot fixture.
///
/// Run with: cargo test --test migration_parity regenerate_schema_snapshot -- --ignored
#[test]
#[ignore]
fn regenerate_schema_snapshot() {
    let conn = Connection::open_in_memory().expect("Failed to open DB");
    run_cetane_migrations(&conn).expect("Failed to run cetane migrations");

    let snapshot = extract_snapshot(&conn);
    let json = serde_json::to_string_pretty(&snapshot).expect("Failed to serialize snapshot");

    std::fs::write(SCHEMA_SNAPSHOT_PATH, &json).expect("Failed to write snapshot");

    println!("Schema snapshot written to {}", SCHEMA_SNAPSHOT_PATH);
    println!("  Tables: {}", snapshot.tables.len());
    println!("  Indexes: {}", snapshot.indexes.len());
    println!("  Triggers: {}", snapshot.triggers.len());
}
