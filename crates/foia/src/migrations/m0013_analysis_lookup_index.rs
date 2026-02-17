use cetane::prelude::*;

pub fn migration() -> Migration {
    Migration::new("0013_analysis_lookup_index")
        .depends_on(&["0003_analysis_results"])
        .operation(AddIndex::new(
            "document_analysis_results",
            Index::new("idx_dar_doc_version_type_status")
                .column("document_id")
                .column("version_id")
                .column("analysis_type")
                .column("status"),
        ))
}
