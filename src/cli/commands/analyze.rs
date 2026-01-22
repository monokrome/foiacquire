//! Document analysis commands (MIME detection, text extraction, OCR).

use std::sync::Arc;

use console::style;
use indicatif::{ProgressBar, ProgressStyle};

use crate::config::{Config, Settings};
use crate::ocr::TextExtractor;

use super::helpers::truncate;
use super::scrape::ReloadMode;

/// Check analysis tool availability.
pub async fn cmd_analyze_check() -> anyhow::Result<()> {
    use crate::ocr::{DeepSeekBackend, OcrBackend, TesseractBackend};

    println!("\n{}", style("OCR Tool Status").bold());
    println!("{}", "-".repeat(50));

    // Check legacy tools
    let tools = TextExtractor::check_tools();
    println!("\n{}", style("Traditional Tools:").cyan());
    let mut all_found = true;

    for (tool, available) in &tools {
        let status = if *available {
            style("✓ found").green()
        } else {
            all_found = false;
            style("✗ not found").red()
        };
        println!("  {:<15} {}", tool, status);
    }

    // Check new backends
    println!("\n{}", style("OCR Backends:").cyan());

    // Tesseract (always available)
    let tesseract = TesseractBackend::new();
    let tesseract_status = if tesseract.is_available() {
        style("✓ available").green()
    } else {
        style("✗ not available").red()
    };
    println!("  {:<15} {}", "Tesseract", tesseract_status);
    if !tesseract.is_available() {
        println!(
            "                  {}",
            style(tesseract.availability_hint()).dim()
        );
    }

    // OCRS (models auto-download on first use)
    #[cfg(feature = "ocr-ocrs")]
    {
        use crate::ocr::OcrsBackend;
        let ocrs = OcrsBackend::new();
        let ocrs_status = if ocrs.is_available() {
            style("✓ available").green()
        } else {
            style("○ models will auto-download").yellow()
        };
        println!("  {:<15} {}", "OCRS", ocrs_status);
        println!(
            "                  {}",
            style(ocrs.availability_hint()).dim()
        );
    }
    #[cfg(not(feature = "ocr-ocrs"))]
    {
        println!(
            "  {:<15} {}",
            "OCRS",
            style("not compiled (enable ocr-ocrs feature)").dim()
        );
    }

    // PaddleOCR (models auto-download on first use)
    #[cfg(feature = "ocr-paddle")]
    {
        use crate::ocr::PaddleBackend;
        let paddle = PaddleBackend::new();
        let paddle_status = if paddle.is_available() {
            style("✓ available").green()
        } else {
            style("○ models will auto-download").yellow()
        };
        println!("  {:<15} {}", "PaddleOCR", paddle_status);
        println!(
            "                  {}",
            style(paddle.availability_hint()).dim()
        );
    }
    #[cfg(not(feature = "ocr-paddle"))]
    {
        println!(
            "  {:<15} {}",
            "PaddleOCR",
            style("not compiled (enable ocr-paddle feature)").dim()
        );
    }

    // DeepSeek (always available but requires binary)
    let deepseek = DeepSeekBackend::new();
    let deepseek_status = if deepseek.is_available() {
        style("✓ available").green()
    } else {
        style("○ not installed").yellow()
    };
    println!("  {:<15} {}", "DeepSeek", deepseek_status);
    if !deepseek.is_available() {
        println!(
            "                  {}",
            style("Install: https://github.com/TimmyOVO/deepseek-ocr.rs").dim()
        );
    }

    // Show default backend
    println!("\n{}", style("Default Backend:").cyan());
    if tesseract.is_available() {
        println!("  {} Tesseract (used for all sources)", style("→").green());
    } else {
        println!(
            "  {} None available - install tesseract-ocr",
            style("!").yellow()
        );
    }
    println!(
        "  {}",
        style("Note: Per-source OCR backend config not yet available").dim()
    );

    println!();

    if all_found {
        println!("{} Basic OCR tools are available", style("✓").green());
    } else {
        println!(
            "{} Some tools are missing. Install them for full OCR support:",
            style("!").yellow()
        );
        println!("  - pdftotext, pdftoppm, pdfinfo: poppler-utils package");
        println!("  - tesseract: tesseract-ocr package");
    }

    Ok(())
}

/// Get PDF page count using pdfinfo.
fn get_pdf_page_count(file: &std::path::Path) -> anyhow::Result<u32> {
    use std::process::Command;
    let output = Command::new("pdfinfo").arg(file).output()?;

    if !output.status.success() {
        anyhow::bail!("pdfinfo failed");
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        if line.starts_with("Pages:") {
            let count = line
                .split(':')
                .nth(1)
                .and_then(|s| s.trim().parse::<u32>().ok())
                .unwrap_or(1);
            return Ok(count);
        }
    }
    Ok(1)
}

/// Parse a page range string like "1", "1-5", "1,3,5-10" into a list of page numbers.
fn parse_page_range(range_str: &str, max_pages: u32) -> Vec<u32> {
    let mut pages = Vec::new();

    for part in range_str.split(',') {
        let part = part.trim();
        if part.contains('-') {
            // Range like "1-5"
            let mut iter = part.split('-');
            let start: u32 = iter.next().and_then(|s| s.trim().parse().ok()).unwrap_or(1);
            let end: u32 = iter
                .next()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(max_pages);
            for p in start..=end.min(max_pages) {
                if p >= 1 && !pages.contains(&p) {
                    pages.push(p);
                }
            }
        } else if let Ok(p) = part.parse::<u32>() {
            if p >= 1 && p <= max_pages && !pages.contains(&p) {
                pages.push(p);
            }
        }
    }

    pages.sort();
    pages
}

/// Per-page OCR result for comparison.
#[derive(Clone)]
struct PageResult {
    text: String,
}

/// Backend configuration for comparison (includes device setting).
struct BackendConfig {
    name: String,
    backend_type: crate::ocr::OcrBackendType,
    use_gpu: bool,
}

/// Parse backend string into configurations.
/// Syntax: backend[:device] where device is 'gpu' or 'cpu'
/// Examples: tesseract, deepseek:gpu, deepseek:cpu, paddleocr:gpu
/// Defaults: deepseek -> gpu, others -> cpu
fn parse_backend_configs(backends_str: &str) -> Result<Vec<BackendConfig>, String> {
    use crate::ocr::OcrBackendType;

    let mut configs = Vec::new();
    for spec in backends_str.split(',').map(|s| s.trim()) {
        let (backend_name, device) = if let Some(idx) = spec.find(':') {
            let (name, dev) = spec.split_at(idx);
            (name, Some(&dev[1..])) // Skip the ':'
        } else {
            (spec, None)
        };

        let backend_name_lower = backend_name.to_lowercase();
        let Some(backend_type) = OcrBackendType::from_str(&backend_name_lower) else {
            return Err(format!(
                "Unknown backend '{}'. Available: tesseract, ocrs, paddleocr, deepseek",
                backend_name
            ));
        };

        // Determine GPU setting based on device flag and backend capabilities
        let use_gpu = match device.map(|d| d.to_lowercase()).as_deref() {
            Some("gpu") => {
                // Check if backend supports GPU
                match backend_type {
                    OcrBackendType::Tesseract => {
                        return Err("tesseract does not support GPU acceleration".to_string());
                    }
                    #[cfg(feature = "ocr-ocrs")]
                    OcrBackendType::Ocrs => {
                        return Err("ocrs does not support GPU acceleration".to_string());
                    }
                    _ => true,
                }
            }
            Some("cpu") => false,
            Some(other) => {
                return Err(format!("Unknown device '{}'. Use :gpu or :cpu", other));
            }
            None => {
                // Defaults: deepseek -> GPU (CPU is impractically slow), others -> CPU
                matches!(backend_type, OcrBackendType::DeepSeek)
            }
        };

        let display_name = if device.is_some() {
            spec.to_string()
        } else if use_gpu {
            format!("{}:gpu", backend_name_lower)
        } else {
            backend_name_lower.to_string()
        };

        configs.push(BackendConfig {
            name: display_name,
            backend_type,
            use_gpu,
        });
    }
    Ok(configs)
}

/// Compare OCR backends on an image or PDF.
pub async fn cmd_analyze_compare(
    file: &std::path::Path,
    pages_str: Option<&str>,
    backends_str: &str,
    deepseek_path: Option<std::path::PathBuf>,
) -> anyhow::Result<()> {
    use crate::ocr::{DeepSeekBackend, OcrBackend, OcrBackendType, OcrConfig, TesseractBackend};
    use std::collections::HashMap;

    if !file.exists() {
        anyhow::bail!("File not found: {}", file.display());
    }

    // Determine file type
    let is_pdf = file
        .extension()
        .map(|e| e.to_string_lossy().to_lowercase() == "pdf")
        .unwrap_or(false);

    // Get pages to process
    let pages: Vec<u32> = if is_pdf {
        let total_pages = get_pdf_page_count(file).unwrap_or(1);
        match pages_str {
            Some(range) => parse_page_range(range, total_pages),
            None => (1..=total_pages).collect(), // All pages by default
        }
    } else {
        vec![1] // Images only have one "page"
    };

    // Parse requested backends with their configurations
    let backend_configs =
        parse_backend_configs(backends_str).map_err(|e| anyhow::anyhow!("{}", e))?;

    if backend_configs.is_empty() {
        anyhow::bail!("No valid backends specified");
    }

    // Progress to stderr
    eprintln!(
        "Processing {} with {} backend(s) across {} page(s)...",
        file.display(),
        backend_configs.len(),
        pages.len()
    );

    // Store per-page results for each backend: backend_name -> page_num -> PageResult
    let mut all_results: HashMap<String, HashMap<u32, PageResult>> = HashMap::new();
    let mut errors: HashMap<String, String> = HashMap::new();
    let mut total_times: HashMap<String, u64> = HashMap::new();

    for config in &backend_configs {
        let backend_name = config.name.clone();
        let mut page_results: HashMap<u32, PageResult> = HashMap::new();
        let mut total_time_ms: u64 = 0;
        let mut had_error = false;

        eprint!("  {} ", backend_name);
        use std::io::Write;
        std::io::stderr().flush().ok();

        for (i, &page) in pages.iter().enumerate() {
            let result = match config.backend_type {
                OcrBackendType::Tesseract => {
                    let backend = TesseractBackend::new();
                    if !backend.is_available() {
                        errors.insert(backend_name.clone(), backend.availability_hint());
                        had_error = true;
                        break;
                    }
                    if is_pdf {
                        backend.ocr_pdf_page(file, page)
                    } else {
                        backend.ocr_image(file)
                    }
                }
                OcrBackendType::DeepSeek => {
                    let ocr_config = OcrConfig {
                        use_gpu: config.use_gpu,
                        ..Default::default()
                    };
                    let mut backend = DeepSeekBackend::with_config(ocr_config);
                    if let Some(ref path) = deepseek_path {
                        backend = backend.with_binary_path(path);
                    }
                    if config.use_gpu {
                        backend = backend.with_device("cuda").with_dtype("f16");
                    }
                    if !backend.is_available() {
                        errors.insert(backend_name.clone(), backend.availability_hint());
                        had_error = true;
                        break;
                    }
                    if is_pdf {
                        backend.ocr_pdf_page(file, page)
                    } else {
                        backend.ocr_image(file)
                    }
                }
                #[cfg(feature = "ocr-ocrs")]
                OcrBackendType::Ocrs => {
                    use crate::ocr::OcrsBackend;
                    let backend = OcrsBackend::new();
                    if is_pdf {
                        backend.ocr_pdf_page(file, page)
                    } else {
                        backend.ocr_image(file)
                    }
                }
                #[cfg(not(feature = "ocr-ocrs"))]
                OcrBackendType::Ocrs => {
                    errors.insert(
                        backend_name.clone(),
                        "OCRS not compiled (enable ocr-ocrs feature)".to_string(),
                    );
                    had_error = true;
                    break;
                }
                #[cfg(feature = "ocr-paddle")]
                OcrBackendType::PaddleOcr => {
                    use crate::ocr::PaddleBackend;
                    let backend = PaddleBackend::new();
                    if is_pdf {
                        backend.ocr_pdf_page(file, page)
                    } else {
                        backend.ocr_image(file)
                    }
                }
                #[cfg(not(feature = "ocr-paddle"))]
                OcrBackendType::PaddleOcr => {
                    errors.insert(
                        backend_name.clone(),
                        "PaddleOCR not compiled (enable ocr-paddle feature)".to_string(),
                    );
                    had_error = true;
                    break;
                }
                OcrBackendType::Gemini => {
                    use crate::ocr::GeminiBackend;
                    let backend = GeminiBackend::new();
                    if !backend.is_available() {
                        errors.insert(backend_name.clone(), backend.availability_hint());
                        had_error = true;
                        break;
                    }
                    if is_pdf {
                        backend.ocr_pdf_page(file, page)
                    } else {
                        backend.ocr_image(file)
                    }
                }
                OcrBackendType::Groq => {
                    use crate::ocr::GroqBackend;
                    let backend = GroqBackend::new();
                    if !backend.is_available() {
                        errors.insert(backend_name.clone(), backend.availability_hint());
                        had_error = true;
                        break;
                    }
                    if is_pdf {
                        backend.ocr_pdf_page(file, page)
                    } else {
                        backend.ocr_image(file)
                    }
                }
            };

            match result {
                Ok(ocr_result) => {
                    total_time_ms += ocr_result.processing_time_ms;
                    page_results.insert(
                        page,
                        PageResult {
                            text: ocr_result.text,
                        },
                    );
                    // Progress dots to stderr
                    if (i + 1) % 10 == 0 {
                        eprint!(".");
                        std::io::stderr().flush().ok();
                    }
                }
                Err(e) => {
                    errors.insert(backend_name.clone(), format!("Page {}: {}", page, e));
                    had_error = true;
                    break;
                }
            }
        }

        if had_error {
            eprintln!(" error");
        } else {
            eprintln!(" done ({}ms)", total_time_ms);
            all_results.insert(backend_name.clone(), page_results);
            total_times.insert(backend_name, total_time_ms);
        }
    }

    if all_results.is_empty() {
        eprintln!("No backends produced results");
        for (name, err) in &errors {
            eprintln!("  {}: {}", name, err);
        }
        return Ok(());
    }

    eprintln!();

    // Get ordered list of backends that succeeded
    let backend_names: Vec<&String> = all_results.keys().collect();
    let num_backends = backend_names.len();

    // Calculate column width based on number of backends
    let total_width = 120;
    let col_width = if num_backends > 0 {
        (total_width - num_backends - 1) / num_backends
    } else {
        40
    };

    // === PAGE-BY-PAGE DIFF ===
    println!("{}", "═".repeat(total_width));
    println!("OCR Comparison: {}", file.display());
    println!("{}", "═".repeat(total_width));

    for &page in &pages {
        println!("\n{}", style(format!("── Page {} ", page)).bold());
        println!("{}", "─".repeat(total_width));

        // Print header row with backend names
        for (i, name) in backend_names.iter().enumerate() {
            if i > 0 {
                print!("│");
            }
            print!("{:^width$}", style(*name).cyan().bold(), width = col_width);
        }
        println!();

        // Separator
        for i in 0..num_backends {
            if i > 0 {
                print!("┼");
            }
            print!("{}", "─".repeat(col_width));
        }
        println!();

        // Get lines for each backend for this page
        let mut backend_lines: Vec<Vec<&str>> = Vec::new();
        for name in &backend_names {
            let lines: Vec<&str> = all_results
                .get(*name)
                .and_then(|pages| pages.get(&page))
                .map(|r| r.text.lines().collect())
                .unwrap_or_default();
            backend_lines.push(lines);
        }

        let max_lines = backend_lines.iter().map(|l| l.len()).max().unwrap_or(0);

        for line_idx in 0..max_lines {
            // Check if all backends have the same line
            let first_line = backend_lines
                .first()
                .and_then(|l| l.get(line_idx))
                .map(|s| s.trim());
            let all_same = backend_lines
                .iter()
                .all(|lines| lines.get(line_idx).map(|s| s.trim()) == first_line);

            for (backend_idx, lines) in backend_lines.iter().enumerate() {
                if backend_idx > 0 {
                    print!("│");
                }
                let line = lines.get(line_idx).map(|s| s.trim()).unwrap_or("");
                let truncated = truncate(line, col_width);

                if all_same {
                    print!("{:<width$}", truncated, width = col_width);
                } else {
                    // Highlight differences
                    print!("{:<width$}", style(&truncated).yellow(), width = col_width);
                }
            }
            println!();
        }
    }

    // === SUMMARY ===
    println!("\n{}", "═".repeat(total_width));
    println!("{}", style("Summary").bold());
    println!("{}", "─".repeat(total_width));
    println!("File: {}", file.display());
    println!("Pages: {}", pages.len());
    println!();

    // Sort by total time
    let mut timing_vec: Vec<_> = total_times.iter().collect();
    timing_vec.sort_by_key(|(_, ms)| *ms);

    let fastest_ms = timing_vec.first().map(|(_, ms)| **ms).unwrap_or(1);

    println!(
        "{:<12} {:>10} {:>10} {:>10} {:>8}",
        "Backend", "Total", "Per Page", "Chars", ""
    );
    println!("{}", "─".repeat(60));

    for (name, total_ms) in &timing_vec {
        let avg_ms = **total_ms / pages.len() as u64;

        // Count total chars for this backend
        let total_chars: usize = all_results
            .get(*name)
            .map(|pages| {
                pages
                    .values()
                    .map(|r| r.text.chars().filter(|c| !c.is_whitespace()).count())
                    .sum()
            })
            .unwrap_or(0);

        let speedup = if **total_ms == fastest_ms {
            style("fastest").green().to_string()
        } else {
            let ratio = **total_ms as f64 / fastest_ms as f64;
            style(format!("{:.1}x slower", ratio)).yellow().to_string()
        };

        println!(
            "{:<12} {:>8}ms {:>8}ms {:>10} {}",
            name, total_ms, avg_ms, total_chars, speedup
        );
    }

    // Show errors if any
    if !errors.is_empty() {
        println!("\n{}", style("Errors:").red().bold());
        for (name, err) in &errors {
            println!("  {}: {}", name, err);
        }
    }

    println!();
    Ok(())
}

/// Analyze documents: detect MIME types, extract text, and run OCR.
#[allow(clippy::too_many_arguments)]
pub async fn cmd_analyze(
    settings: &Settings,
    source_id: Option<&str>,
    doc_id: Option<&str>,
    method: Option<&str>,
    workers: usize,
    limit: usize,
    mime_type: Option<&str>,
    daemon: bool,
    interval: u64,
    reload: ReloadMode,
) -> anyhow::Result<()> {
    // Parse methods from comma-separated string (e.g., "ocr,whisper")
    let methods: Vec<String> = method
        .map(|m| m.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_else(|| vec!["ocr".to_string()]);
    use crate::services::{AnalysisEvent, AnalysisService};
    use tokio::sync::mpsc;

    // Check for required tools upfront
    let tools = TextExtractor::check_tools();
    let missing: Vec<_> = tools.iter().filter(|(_, avail)| !avail).collect();

    if !missing.is_empty() {
        println!("{} Required OCR tools are missing:", style("✗").red());
        for (tool, _) in &missing {
            println!("  - {}", tool);
        }
        println!();
        println!("Install the missing tools, then run: foiacquire ocr-check");
        return Err(anyhow::anyhow!(
            "Missing required tools. Run 'foiacquire ocr-check' for install instructions."
        ));
    }

    // Set up config watcher for stop-process and inplace modes
    // Try file watching first, fall back to DB polling if no config file
    let mut config_watcher =
        if daemon && matches!(reload, ReloadMode::StopProcess | ReloadMode::Inplace) {
            prefer::watch("foiacquire").await.ok()
        } else {
            None
        };

    let ctx = settings.create_db_context()?;
    let doc_repo = ctx.documents();
    let config_history = ctx.config_history();

    // Track config hash for DB-based change detection
    let config = Config::load().await;
    let mut current_config_hash = config.hash();

    let service = AnalysisService::new(doc_repo);

    // If specific doc_id provided, process just that document (no daemon mode)
    if let Some(id) = doc_id {
        println!("{} Processing single document: {}", style("→").cyan(), id);
        let (event_tx, _event_rx) = mpsc::channel::<AnalysisEvent>(100);
        return service.process_single(id, event_tx).await;
    }

    if daemon {
        println!(
            "{} Running in daemon mode (interval: {}s, reload: {:?})",
            style("→").cyan(),
            interval,
            reload
        );
    }

    loop {
        // Check if there's work to do
        let (docs_count, pages_count) = service.count_needing_processing(source_id, mime_type).await?;
        if docs_count == 0 && pages_count == 0 {
            if daemon {
                println!(
                    "{} No documents need OCR processing, sleeping for {}s...",
                    style("→").dim(),
                    interval
                );
                tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
                continue;
            } else {
                println!("{} No documents need OCR processing", style("!").yellow());
                return Ok(());
            }
        }

        // Create event channel for progress tracking
        let (event_tx, mut event_rx) = mpsc::channel::<AnalysisEvent>(100);

        // State for progress bar
        let pb = Arc::new(tokio::sync::Mutex::new(None::<ProgressBar>));
        let pb_clone = pb.clone();

        // Spawn event handler for UI
        let event_handler = tokio::spawn(async move {
            let mut phase1_succeeded = 0;
            let mut phase1_failed = 0;
            let mut phase1_pages = 0;
            let mut phase2_improved = 0;
            let mut phase2_skipped = 0;
            let mut phase2_failed = 0;
            let mut docs_finalized_incremental = 0;

            while let Some(event) = event_rx.recv().await {
                match event {
                    AnalysisEvent::Phase1Started { total_documents } => {
                        println!(
                            "{} Phase 1: Extracting text from {} documents",
                            style("→").cyan(),
                            total_documents
                        );
                        let progress = ProgressBar::new(total_documents as u64);
                        progress.set_style(
                            ProgressStyle::default_bar()
                                .template(
                                    "{spinner:.green} [{bar:30.cyan/blue}] {pos}/{len} {wide_msg}",
                                )
                                .unwrap()
                                .progress_chars("█▓░"),
                        );
                        progress.set_message("Extracting text...");
                        *pb_clone.lock().await = Some(progress);
                    }
                    AnalysisEvent::DocumentCompleted {
                        pages_extracted, ..
                    } => {
                        phase1_succeeded += 1;
                        phase1_pages += pages_extracted;
                        if let Some(ref progress) = *pb_clone.lock().await {
                            progress.inc(1);
                        }
                    }
                    AnalysisEvent::DocumentFailed { document_id, error } => {
                        phase1_failed += 1;
                        if let Some(ref progress) = *pb_clone.lock().await {
                            progress.suspend(|| {
                                eprintln!(
                                    "  {} Document {} failed: {}",
                                    style("✗").red(),
                                    document_id,
                                    error
                                );
                            });
                            progress.inc(1);
                        } else {
                            eprintln!(
                                "  {} Document {} failed: {}",
                                style("✗").red(),
                                document_id,
                                error
                            );
                        }
                    }
                    AnalysisEvent::Phase1Complete { .. } => {
                        if let Some(ref progress) = *pb_clone.lock().await {
                            progress.finish_and_clear();
                        }
                        *pb_clone.lock().await = None;
                        println!(
                            "{} Phase 1 complete: {} documents processed, {} pages extracted",
                            style("✓").green(),
                            phase1_succeeded,
                            phase1_pages
                        );
                        if phase1_failed > 0 {
                            println!(
                                "  {} {} documents failed",
                                style("!").yellow(),
                                phase1_failed
                            );
                        }
                    }
                    AnalysisEvent::Phase2Started { total_pages } => {
                        println!(
                            "{} Phase 2: Running OCR on {} pages",
                            style("→").cyan(),
                            total_pages
                        );
                        let progress = ProgressBar::new(total_pages as u64);
                        progress.set_style(
                            ProgressStyle::default_bar()
                                .template(
                                    "{spinner:.green} [{bar:30.cyan/blue}] {pos}/{len} {wide_msg}",
                                )
                                .unwrap()
                                .progress_chars("█▓░"),
                        );
                        progress.set_message("Running OCR...");
                        *pb_clone.lock().await = Some(progress);
                    }
                    AnalysisEvent::PageOcrCompleted { improved, .. } => {
                        if improved {
                            phase2_improved += 1;
                        } else {
                            phase2_skipped += 1;
                        }
                        if let Some(ref progress) = *pb_clone.lock().await {
                            progress.inc(1);
                        }
                    }
                    AnalysisEvent::PageOcrFailed {
                        document_id,
                        page_number,
                        error,
                    } => {
                        phase2_failed += 1;
                        if let Some(ref progress) = *pb_clone.lock().await {
                            progress.suspend(|| {
                                eprintln!(
                                    "  {} Page {} of {} failed: {}",
                                    style("✗").red(),
                                    page_number,
                                    document_id,
                                    error
                                );
                            });
                            progress.inc(1);
                        } else {
                            eprintln!(
                                "  {} Page {} of {} failed: {}",
                                style("✗").red(),
                                page_number,
                                document_id,
                                error
                            );
                        }
                    }
                    AnalysisEvent::DocumentFinalized { .. } => {
                        docs_finalized_incremental += 1;
                        if let Some(ref progress) = *pb_clone.lock().await {
                            progress.set_message(format!(
                                "{} docs complete",
                                docs_finalized_incremental
                            ));
                        }
                    }
                    AnalysisEvent::Phase2Complete { .. } => {
                        if let Some(ref progress) = *pb_clone.lock().await {
                            progress.finish_and_clear();
                        }
                        *pb_clone.lock().await = None;
                        let mut msg = format!(
                            "{} Phase 2 complete: {} pages improved by OCR, {} kept PDF text",
                            style("✓").green(),
                            phase2_improved,
                            phase2_skipped
                        );
                        if phase2_failed > 0 {
                            msg.push_str(&format!(", {} failed", phase2_failed));
                        }
                        if docs_finalized_incremental > 0 {
                            msg.push_str(&format!(
                                ", {} documents finalized",
                                docs_finalized_incremental
                            ));
                        }
                        println!("{}", msg);
                    }
                    _ => {}
                }
            }
        });

        // Run service
        let _result = service
            .process(source_id, &methods, workers, limit, mime_type, event_tx)
            .await?;

        // Wait for event handler to finish
        if let Err(e) = event_handler.await {
            tracing::warn!("Event handler task failed: {}", e);
        }

        if !daemon {
            break;
        }

        // Sleep with config watching for stop-process and inplace modes
        println!(
            "{} Sleeping for {}s before next check...",
            style("→").dim(),
            interval
        );

        if let Some(ref mut watcher) = config_watcher {
            // File-based config watching
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(interval)) => {}
                result = watcher.recv() => {
                    if result.is_some() {
                        match reload {
                            ReloadMode::StopProcess => {
                                println!(
                                    "{} Config file changed, exiting for restart...",
                                    style("↻").cyan()
                                );
                                return Ok(());
                            }
                            ReloadMode::Inplace => {
                                println!(
                                    "{} Config file changed, continuing...",
                                    style("↻").cyan()
                                );
                                // OCR doesn't use config, so just continue
                            }
                            ReloadMode::NextRun => {}
                        }
                    }
                }
            }
        } else if daemon && matches!(reload, ReloadMode::StopProcess | ReloadMode::Inplace) {
            // DB-based config polling (no config file available)
            tokio::time::sleep(std::time::Duration::from_secs(interval)).await;

            // Check if config changed in DB
            if let Ok(Some(latest_hash)) = config_history.get_latest_hash().await {
                if latest_hash != current_config_hash {
                    match reload {
                        ReloadMode::StopProcess => {
                            println!(
                                "{} Config changed in database, exiting for restart...",
                                style("↻").cyan()
                            );
                            return Ok(());
                        }
                        ReloadMode::Inplace => {
                            println!(
                                "{} Config changed in database, continuing...",
                                style("↻").cyan()
                            );
                            current_config_hash = latest_hash;
                        }
                        ReloadMode::NextRun => {}
                    }
                }
            }
        } else {
            tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
        }
    }

    Ok(())
}
