//! Email parsing for extracting attachments and text from RFC822 emails.
//!
//! This module provides functionality to:
//! - Parse email files (.eml / message/rfc822)
//! - Extract attachments to temporary locations for OCR processing
//! - Extract email body text

#![allow(dead_code)]

use mail_parser::{MessageParser, MimeHeaders};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use thiserror::Error;

/// Errors that can occur during email operations.
#[derive(Debug, Error)]
pub enum EmailError {
    #[error("Failed to read email file: {0}")]
    ReadFailed(String),

    #[error("Failed to parse email: {0}")]
    ParseFailed(String),

    #[error("Failed to extract attachment: {0}")]
    ExtractFailed(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Information about an attachment within an email.
#[derive(Debug, Clone)]
pub struct EmailAttachment {
    /// Original filename of the attachment.
    pub filename: String,
    /// MIME type of the attachment.
    pub mime_type: String,
    /// Size in bytes.
    pub size: u64,
    /// Content-ID if available (for inline attachments).
    pub content_id: Option<String>,
}

impl EmailAttachment {
    /// Check if this attachment type is supported for text extraction.
    pub fn is_extractable(&self) -> bool {
        matches!(
            self.mime_type.as_str(),
            "application/pdf"
                | "image/png"
                | "image/jpeg"
                | "image/tiff"
                | "image/gif"
                | "image/bmp"
                | "text/plain"
                | "text/html"
        )
    }
}

/// Result of extracting an attachment from an email.
pub struct ExtractedAttachment {
    /// The attachment information.
    pub attachment: EmailAttachment,
    /// Temporary directory containing the extracted file.
    pub temp_dir: TempDir,
    /// Path to the extracted file.
    pub file_path: PathBuf,
}

/// Parsed email information.
pub struct ParsedEmail {
    /// Email subject.
    pub subject: Option<String>,
    /// Sender address.
    pub from: Option<String>,
    /// Recipient addresses.
    pub to: Vec<String>,
    /// Email date.
    pub date: Option<String>,
    /// Plain text body.
    pub body_text: Option<String>,
    /// HTML body.
    pub body_html: Option<String>,
    /// List of attachments.
    pub attachments: Vec<EmailAttachment>,
}

/// Email parser for RFC822 (.eml) files.
pub struct EmailExtractor;

/// Read and parse an email file into a mail_parser Message.
fn read_and_parse_email(email_path: &Path) -> Result<Vec<u8>, EmailError> {
    let mut file = File::open(email_path).map_err(|e| EmailError::ReadFailed(e.to_string()))?;
    let mut raw_email = Vec::new();
    file.read_to_end(&mut raw_email)
        .map_err(|e| EmailError::ReadFailed(e.to_string()))?;
    Ok(raw_email)
}

/// Extract MIME type from a content type, defaulting to octet-stream.
fn mime_type_from_content_type(ct: Option<&mail_parser::ContentType>) -> String {
    ct.map(|ct| {
        if let Some(subtype) = ct.subtype() {
            format!("{}/{}", ct.ctype(), subtype)
        } else {
            ct.ctype().to_string()
        }
    })
    .unwrap_or_else(|| "application/octet-stream".to_string())
}

/// Build an EmailAttachment from attachment metadata.
fn build_attachment_info(
    filename: &str,
    attachment: &mail_parser::MessagePart,
) -> EmailAttachment {
    let mime_type = mime_type_from_content_type(attachment.content_type());
    let size = attachment.contents().len() as u64;
    let content_id = attachment.content_id().map(|s| s.to_string());

    EmailAttachment {
        filename: filename.to_string(),
        mime_type,
        size,
        content_id,
    }
}

impl EmailExtractor {
    /// Check if a MIME type represents an email format.
    pub fn is_email(mime_type: &str) -> bool {
        mime_type == "message/rfc822"
    }

    /// Parse an email file and return its metadata and attachment list.
    pub fn parse_email(email_path: &Path) -> Result<ParsedEmail, EmailError> {
        let raw_email = read_and_parse_email(email_path)?;
        let message = MessageParser::default()
            .parse(&raw_email)
            .ok_or_else(|| EmailError::ParseFailed("Failed to parse email".to_string()))?;

        // Extract basic headers
        let subject = message.subject().map(|s| s.to_string());
        let from = message.from().and_then(|addrs| {
            addrs.first().map(|addr| {
                if let Some(name) = addr.name() {
                    format!("{} <{}>", name, addr.address().unwrap_or_default())
                } else {
                    addr.address().unwrap_or_default().to_string()
                }
            })
        });

        let to: Vec<String> = message
            .to()
            .map(|addrs| {
                addrs
                    .iter()
                    .map(|addr| addr.address().unwrap_or_default().to_string())
                    .collect()
            })
            .unwrap_or_default();

        let date = message.date().map(|d| d.to_rfc3339());

        // Extract body text
        let body_text = message.body_text(0).map(|s| s.to_string());
        let body_html = message.body_html(0).map(|s| s.to_string());

        // Extract attachment info
        let attachments: Vec<EmailAttachment> = message
            .attachments()
            .filter_map(|attachment| {
                attachment
                    .attachment_name()
                    .map(|filename| build_attachment_info(filename, attachment))
            })
            .collect();

        Ok(ParsedEmail {
            subject,
            from,
            to,
            date,
            body_text,
            body_html,
            attachments,
        })
    }

    /// List all attachments in an email file.
    pub fn list_attachments(email_path: &Path) -> Result<Vec<EmailAttachment>, EmailError> {
        let parsed = Self::parse_email(email_path)?;
        Ok(parsed.attachments)
    }

    /// Extract a single attachment from an email to a temporary location.
    pub fn extract_attachment(
        email_path: &Path,
        attachment_filename: &str,
    ) -> Result<ExtractedAttachment, EmailError> {
        let raw_email = read_and_parse_email(email_path)?;
        let message = MessageParser::default()
            .parse(&raw_email)
            .ok_or_else(|| EmailError::ParseFailed("Failed to parse email".to_string()))?;

        // Find the attachment by filename
        for attachment in message.attachments() {
            if let Some(filename) = attachment.attachment_name() {
                if filename == attachment_filename {
                    return Self::extract_attachment_to_temp(filename, attachment);
                }
            }
        }

        Err(EmailError::ExtractFailed(format!(
            "Attachment '{}' not found in email",
            attachment_filename
        )))
    }

    /// Extract an attachment part to a temporary file.
    fn extract_attachment_to_temp(
        filename: &str,
        attachment: &mail_parser::MessagePart,
    ) -> Result<ExtractedAttachment, EmailError> {
        let attachment_info = build_attachment_info(filename, attachment);
        let contents = attachment.contents();

        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join(filename);

        let mut outfile = File::create(&file_path)?;
        outfile.write_all(contents)?;

        Ok(ExtractedAttachment {
            attachment: attachment_info,
            temp_dir,
            file_path,
        })
    }

    /// Extract all attachments from an email.
    pub fn extract_all_attachments(
        email_path: &Path,
    ) -> Result<Vec<ExtractedAttachment>, EmailError> {
        let raw_email = read_and_parse_email(email_path)?;
        let message = MessageParser::default()
            .parse(&raw_email)
            .ok_or_else(|| EmailError::ParseFailed("Failed to parse email".to_string()))?;

        let mut extracted = Vec::new();
        for attachment in message.attachments() {
            if let Some(filename) = attachment.attachment_name() {
                extracted.push(Self::extract_attachment_to_temp(filename, attachment)?);
            }
        }

        Ok(extracted)
    }

    /// Get the combined text content from an email (body + any text attachments).
    pub fn get_email_text(email_path: &Path) -> Result<String, EmailError> {
        let parsed = Self::parse_email(email_path)?;

        let mut text = String::new();

        // Add headers
        if let Some(subject) = &parsed.subject {
            text.push_str(&format!("Subject: {}\n", subject));
        }
        if let Some(from) = &parsed.from {
            text.push_str(&format!("From: {}\n", from));
        }
        if !parsed.to.is_empty() {
            text.push_str(&format!("To: {}\n", parsed.to.join(", ")));
        }
        if let Some(date) = &parsed.date {
            text.push_str(&format!("Date: {}\n", date));
        }

        text.push('\n');

        // Add body text
        if let Some(body) = &parsed.body_text {
            text.push_str(body);
        } else if let Some(html) = &parsed.body_html {
            // Simple HTML to text conversion - just strip tags
            let stripped = html
                .replace("<br>", "\n")
                .replace("<br/>", "\n")
                .replace("<br />", "\n")
                .replace("</p>", "\n\n")
                .replace("</div>", "\n");

            // Remove remaining HTML tags
            let re = regex::Regex::new(r"<[^>]+>").unwrap();
            let plain = re.replace_all(&stripped, "");
            text.push_str(&plain);
        }

        // Note attachments
        if !parsed.attachments.is_empty() {
            text.push_str("\n\n--- Attachments ---\n");
            for att in &parsed.attachments {
                text.push_str(&format!(
                    "- {} ({}, {} bytes)\n",
                    att.filename, att.mime_type, att.size
                ));
            }
        }

        Ok(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_email() {
        assert!(EmailExtractor::is_email("message/rfc822"));
        assert!(!EmailExtractor::is_email("application/pdf"));
    }

    #[test]
    fn test_attachment_is_extractable() {
        let pdf = EmailAttachment {
            filename: "test.pdf".to_string(),
            mime_type: "application/pdf".to_string(),
            size: 1000,
            content_id: None,
        };
        assert!(pdf.is_extractable());

        let doc = EmailAttachment {
            filename: "test.doc".to_string(),
            mime_type: "application/msword".to_string(),
            size: 1000,
            content_id: None,
        };
        assert!(!doc.is_extractable());
    }
}
