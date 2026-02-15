//! Default LLM prompts for document analysis.

/// Default prompt for generating document synopsis.
pub const DEFAULT_SYNOPSIS_PROMPT: &str = r#"You are analyzing a FOIA (Freedom of Information Act) document. Read the ENTIRE content and identify the MAIN SUBJECT and KEY FINDINGS - not just what's in the introduction.

Your synopsis should answer:
1. What is this document ABOUT? (the central topic or investigation)
2. What are the KEY FACTS revealed? (dates, names, actions, decisions)
3. Why is this document SIGNIFICANT? (what does it reveal or document?)

IMPORTANT: Do NOT just summarize the first paragraph. Scan the WHOLE document for the most important information. If the document discusses multiple topics, focus on the PRIMARY subject.

Document Title: {title}

Document Content:
{content}

Respond with ONLY a 2-3 sentence synopsis focusing on the document's main subject and key revelations. No formatting or preamble."#;

/// Default prompt for generating document tags.
pub const DEFAULT_TAGS_PROMPT: &str = r#"You are analyzing a FOIA document to generate USEFUL SEARCH TAGS. Read the ENTIRE document before tagging.

Generate 3-5 simple, lowercase tags that capture:
- Government agencies involved (e.g., cia, fbi, nsa, state-dept)
- Main subject matter (e.g., surveillance, assassination, nuclear-weapons)
- Specific programs or operations mentioned (e.g., mkultra, cointelpro, phoenix)
- Key entities or people if significant (e.g., castro, soviet-union, aclu)
- Document type if notable (e.g., memo, cable, briefing)

CRITICAL INSTRUCTIONS:
1. Read BEYOND the first paragraph - the main topic is often revealed deeper in the document
2. Be SPECIFIC - "soviet-intelligence" is better than "foreign-policy"
3. Focus on what makes this document FINDABLE - what would someone search for?
4. Use lowercase with hyphens for multi-word tags (e.g., cold-war, mind-control)
5. Avoid vague tags like "government", "information", "document" - be precise
6. Do NOT use prefixes like "agency:" or "topic:" - just the tag itself

Document Title: {title}

Document Content:
{content}

Respond with ONLY 3-5 comma-separated lowercase tags. Example: cia, mind-control, mkultra, memo, cold-war"#;
