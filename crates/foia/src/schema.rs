// @generated automatically by Diesel CLI.
// Manually corrected to match actual database schema.

diesel::table! {
    configuration_history (uuid) {
        uuid -> Text,
        created_at -> Text,
        data -> Text,
        format -> Text,
        hash -> Text,
    }
}

diesel::table! {
    crawl_config (source_id) {
        source_id -> Text,
        config_hash -> Text,
        updated_at -> Text,
    }
}

diesel::table! {
    crawl_requests (id) {
        id -> Integer,
        source_id -> Text,
        url -> Text,
        method -> Text,
        request_headers -> Text,
        request_at -> Text,
        response_status -> Nullable<Integer>,
        response_headers -> Text,
        response_at -> Nullable<Text>,
        response_size -> Nullable<Integer>,
        duration_ms -> Nullable<Integer>,
        error -> Nullable<Text>,
        was_conditional -> Integer,
        was_not_modified -> Integer,
    }
}

diesel::table! {
    crawl_urls (id) {
        id -> Integer,
        url -> Text,
        source_id -> Text,
        status -> Text,
        discovery_method -> Text,
        parent_url -> Nullable<Text>,
        discovery_context -> Text,
        depth -> Integer,
        discovered_at -> Text,
        fetched_at -> Nullable<Text>,
        retry_count -> Integer,
        last_error -> Nullable<Text>,
        next_retry_at -> Nullable<Text>,
        etag -> Nullable<Text>,
        last_modified -> Nullable<Text>,
        content_hash -> Nullable<Text>,
        document_id -> Nullable<Text>,
    }
}

diesel::table! {
    document_entities (id) {
        id -> Integer,
        document_id -> Text,
        entity_type -> Text,
        entity_text -> Text,
        normalized_text -> Text,
        latitude -> Nullable<Double>,
        longitude -> Nullable<Double>,
        created_at -> Text,
    }
}

diesel::table! {
    document_analysis_results (id) {
        id -> Integer,
        page_id -> Nullable<Integer>,
        document_id -> Text,
        version_id -> Integer,
        analysis_type -> Text,
        backend -> Text,
        result_text -> Nullable<Text>,
        confidence -> Nullable<Float>,
        processing_time_ms -> Nullable<Integer>,
        error -> Nullable<Text>,
        status -> Text,
        created_at -> Text,
        metadata -> Nullable<Text>,
        model -> Nullable<Text>,
    }
}

diesel::table! {
    document_pages (id) {
        id -> Integer,
        document_id -> Text,
        version_id -> Integer,
        page_number -> Integer,
        pdf_text -> Nullable<Text>,
        ocr_text -> Nullable<Text>,
        final_text -> Nullable<Text>,
        ocr_status -> Text,
        created_at -> Text,
        updated_at -> Text,
    }
}

diesel::table! {
    page_ocr_results (id) {
        id -> Integer,
        page_id -> Integer,
        backend -> Text,
        text -> Nullable<Text>,
        confidence -> Nullable<Float>,
        quality_score -> Nullable<Float>,
        char_count -> Nullable<Integer>,
        word_count -> Nullable<Integer>,
        processing_time_ms -> Nullable<Integer>,
        error_message -> Nullable<Text>,
        created_at -> Text,
        model -> Nullable<Text>,
        image_hash -> Nullable<Text>,
    }
}

diesel::table! {
    document_versions (id) {
        id -> Integer,
        document_id -> Text,
        content_hash -> Text,
        content_hash_blake3 -> Nullable<Text>,
        file_path -> Nullable<Text>,
        file_size -> Integer,
        mime_type -> Text,
        acquired_at -> Text,
        source_url -> Nullable<Text>,
        original_filename -> Nullable<Text>,
        server_date -> Nullable<Text>,
        page_count -> Nullable<Integer>,
        archive_snapshot_id -> Nullable<Integer>,
        earliest_archived_at -> Nullable<Text>,
        dedup_index -> Nullable<Integer>,
    }
}

diesel::table! {
    archive_snapshots (id) {
        id -> Integer,
        service -> Text,
        original_url -> Text,
        archive_url -> Text,
        captured_at -> Text,
        discovered_at -> Text,
        http_status -> Nullable<Integer>,
        mimetype -> Nullable<Text>,
        content_length -> Nullable<BigInt>,
        digest -> Nullable<Text>,
        metadata -> Text,
    }
}

diesel::table! {
    archive_checks (id) {
        id -> Integer,
        document_version_id -> Integer,
        archive_source -> Text,
        url_checked -> Text,
        checked_at -> Text,
        snapshots_found -> Integer,
        matching_snapshots -> Integer,
        result -> Text,
        error_message -> Nullable<Text>,
    }
}

diesel::table! {
    documents (id) {
        id -> Text,
        source_id -> Text,
        title -> Text,
        source_url -> Text,
        extracted_text -> Nullable<Text>,
        status -> Text,
        metadata -> Text,
        created_at -> Text,
        updated_at -> Text,
        synopsis -> Nullable<Text>,
        tags -> Nullable<Text>,
        estimated_date -> Nullable<Text>,
        date_confidence -> Nullable<Text>,
        date_source -> Nullable<Text>,
        manual_date -> Nullable<Text>,
        discovery_method -> Text,
        category_id -> Nullable<Text>,
    }
}

diesel::table! {
    rate_limit_state (domain) {
        domain -> Text,
        current_delay_ms -> Integer,
        in_backoff -> Integer,
        total_requests -> Integer,
        rate_limit_hits -> Integer,
        updated_at -> Text,
    }
}

diesel::table! {
    service_status (id) {
        id -> Text,
        service_type -> Text,
        source_id -> Nullable<Text>,
        status -> Text,
        last_heartbeat -> Text,
        last_activity -> Nullable<Text>,
        current_task -> Nullable<Text>,
        stats -> Text,
        started_at -> Text,
        host -> Nullable<Text>,
        version -> Nullable<Text>,
        last_error -> Nullable<Text>,
        last_error_at -> Nullable<Text>,
        error_count -> Integer,
    }
}

diesel::table! {
    sources (id) {
        id -> Text,
        source_type -> Text,
        name -> Text,
        base_url -> Text,
        metadata -> Text,
        created_at -> Text,
        last_scraped -> Nullable<Text>,
    }
}

diesel::table! {
    scraper_configs (source_id) {
        source_id -> Text,
        config -> Text,
        created_at -> Text,
        updated_at -> Text,
    }
}

diesel::table! {
    virtual_files (id) {
        id -> Text,
        document_id -> Text,
        version_id -> Integer,
        archive_path -> Text,
        filename -> Text,
        mime_type -> Text,
        file_size -> Integer,
        extracted_text -> Nullable<Text>,
        synopsis -> Nullable<Text>,
        tags -> Nullable<Text>,
        status -> Text,
        created_at -> Text,
        updated_at -> Text,
    }
}

diesel::joinable!(document_entities -> documents (document_id));
diesel::joinable!(document_pages -> documents (document_id));
diesel::joinable!(document_versions -> documents (document_id));
diesel::joinable!(document_versions -> archive_snapshots (archive_snapshot_id));
diesel::joinable!(documents -> sources (source_id));
diesel::joinable!(virtual_files -> documents (document_id));
diesel::joinable!(page_ocr_results -> document_pages (page_id));

diesel::joinable!(document_analysis_results -> documents (document_id));
diesel::joinable!(document_analysis_results -> document_pages (page_id));
diesel::joinable!(document_analysis_results -> document_versions (version_id));

diesel::joinable!(archive_checks -> document_versions (document_version_id));

diesel::allow_tables_to_appear_in_same_query!(
    archive_checks,
    archive_snapshots,
    configuration_history,
    crawl_config,
    crawl_requests,
    crawl_urls,
    document_analysis_results,
    document_entities,
    document_pages,
    document_versions,
    documents,
    page_ocr_results,
    rate_limit_state,
    scraper_configs,
    service_status,
    sources,
    virtual_files,
);
