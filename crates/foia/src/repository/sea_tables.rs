//! Sea-query Iden enums for tables that use upserts.
//!
//! These provide backend-agnostic table/column identifiers for generating
//! SQL with correct quoting and placeholder syntax via sea-query.
//! Only columns referenced in INSERT/UPDATE/ON CONFLICT clauses need entries.

use sea_query::Iden;

#[derive(Iden)]
pub enum Documents {
    Table,
    Id,
    SourceId,
    SourceUrl,
    Title,
    Status,
    Metadata,
    CreatedAt,
    UpdatedAt,
    CategoryId,
}

#[derive(Iden)]
pub enum DocumentPages {
    Table,
    Id,
    DocumentId,
    VersionId,
    PageNumber,
    PdfText,
    OcrText,
    FinalText,
    OcrStatus,
    CreatedAt,
    UpdatedAt,
}

#[derive(Iden)]
pub enum PageOcrResults {
    Table,
    PageId,
    Backend,
    Text,
    Confidence,
    QualityScore,
    CharCount,
    WordCount,
    ProcessingTimeMs,
    ErrorMessage,
    CreatedAt,
    Model,
    ImageHash,
}

#[derive(Iden)]
pub enum DocumentVersions {
    Table,
    Id,
    DocumentId,
    ContentHash,
    ContentHashBlake3,
    FilePath,
    FileSize,
    MimeType,
    AcquiredAt,
    SourceUrl,
    OriginalFilename,
    ServerDate,
    PageCount,
    ArchiveSnapshotId,
    EarliestArchivedAt,
    DedupIndex,
}

#[derive(Iden)]
pub enum DocumentEntities {
    Table,
    DocumentId,
    EntityType,
    EntityText,
    NormalizedText,
    Latitude,
    Longitude,
    CreatedAt,
}

#[derive(Iden)]
pub enum DocumentAnalysisResults {
    Table,
    Id,
    PageId,
    DocumentId,
    VersionId,
    AnalysisType,
    Backend,
    ResultText,
    Confidence,
    ProcessingTimeMs,
    Error,
    Status,
    CreatedAt,
    Metadata,
    Model,
}

#[derive(Iden)]
pub enum Sources {
    Table,
    Id,
    SourceType,
    Name,
    BaseUrl,
    Metadata,
    CreatedAt,
    LastScraped,
}

#[derive(Iden)]
pub enum ServiceStatusTable {
    #[iden = "service_status"]
    Table,
    Id,
    ServiceType,
    SourceId,
    Status,
    LastHeartbeat,
    LastActivity,
    CurrentTask,
    Stats,
    StartedAt,
    Host,
    Version,
    LastError,
    LastErrorAt,
    ErrorCount,
}

#[derive(Iden)]
pub enum CrawlUrls {
    Table,
    SourceId,
}

#[derive(Iden)]
pub enum CrawlConfig {
    Table,
    SourceId,
}
