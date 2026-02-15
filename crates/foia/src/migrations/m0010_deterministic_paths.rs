use cetane::prelude::*;

pub fn migration() -> Migration {
    Migration::new("0010_deterministic_paths")
        .depends_on(&["0009_document_entities"])
        .operation(AddField::new(
            "document_versions",
            Field::new("explicit_file_path", FieldType::Text),
        ))
        .operation(RunSql::new(
            "UPDATE document_versions SET explicit_file_path = file_path",
        ))
        .operation(RemoveField::new("document_versions", "file_path"))
        .operation(RenameField::new(
            "document_versions",
            "explicit_file_path",
            "file_path",
        ))
        .operation(AddField::new(
            "document_versions",
            Field::new("dedup_index", FieldType::Integer),
        ))
}
