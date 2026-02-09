//! OCR backend comparison command.

use std::collections::HashMap;

use console::style;

use super::super::helpers::truncate;
use super::check::get_pdf_page_count;

/// Per-page OCR result for comparison.
#[derive(Clone)]
struct PageResult {
    text: String,
}

/// Backend configuration for comparison (includes device setting).
struct BackendConfig {
    name: String,
    backend_type: foiacquire_analysis::ocr::OcrBackendType,
    use_gpu: bool,
}

/// Parse backend string into configurations.
/// Syntax: backend[:device] where device is 'gpu' or 'cpu'
/// Examples: tesseract, deepseek:gpu, deepseek:cpu, paddleocr:gpu
/// Defaults: deepseek -> gpu, others -> cpu
fn parse_backend_configs(backends_str: &str) -> Result<Vec<BackendConfig>, String> {
    use foiacquire_analysis::ocr::OcrBackendType;

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

/// Compare OCR backends on an image or PDF.
pub async fn cmd_analyze_compare(
    file: &std::path::Path,
    pages_str: Option<&str>,
    backends_str: &str,
    deepseek_path: Option<std::path::PathBuf>,
) -> anyhow::Result<()> {
    use foiacquire_analysis::ocr::{
        DeepSeekBackend, OcrBackend, OcrBackendType, OcrConfig, TesseractBackend,
    };

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
                    use foiacquire_analysis::ocr::OcrsBackend;
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
                    use foiacquire_analysis::ocr::PaddleBackend;
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
                    use foiacquire_analysis::ocr::GeminiBackend;
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
                    use foiacquire_analysis::ocr::GroqBackend;
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
