//! Analysis tool availability check command.

use console::style;

use foia_analysis::ocr::TextExtractor;

/// Check analysis tool availability.
pub async fn cmd_analyze_check() -> anyhow::Result<()> {
    use foia_analysis::ocr::{DeepSeekBackend, OcrBackend, TesseractBackend};

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
        use foia_analysis::ocr::OcrsBackend;
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
        use foia_analysis::ocr::PaddleBackend;
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
pub fn get_pdf_page_count(file: &std::path::Path) -> anyhow::Result<u32> {
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
