mod m0001_initial;
mod m0002_service_status;
mod m0003_analysis_results;
mod m0004_unique_constraints;
mod m0005_archive_history;
mod m0006_page_ocr_results;
mod m0007_add_model_column;
mod m0008_page_image_hash;

use cetane::prelude::MigrationRegistry;

pub fn registry() -> MigrationRegistry {
    let mut reg = MigrationRegistry::new();
    reg.register(m0001_initial::migration());
    reg.register(m0002_service_status::migration());
    reg.register(m0003_analysis_results::migration());
    reg.register(m0004_unique_constraints::migration());
    reg.register(m0005_archive_history::migration());
    reg.register(m0006_page_ocr_results::migration());
    reg.register(m0007_add_model_column::migration());
    reg.register(m0008_page_image_hash::migration());
    reg
}
