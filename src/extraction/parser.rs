use scraper::{Html, Selector};
use super::chunking::apply_chunking;
use super::filters::is_filtered;
use super::hierarchy::{is_heading, is_paragraph, update_context_from_element};
use super::types::{ChunkMode, ExtractConfig, ExtractionResult, HeadingContext, RawElement};

const MAX_HTML_SIZE: usize = 50 * 1024 * 1024;

pub fn extract_and_chunk(html: &str, config: &ExtractConfig) -> ExtractionResult {
    if html.len() > MAX_HTML_SIZE {
        return ExtractionResult {
            parse_error: Some(format!(
                "Document too large: {} bytes (max {}). Split into chapters.",
                html.len(),
                MAX_HTML_SIZE
            )),
            ..Default::default()
        };
    }


    if html.starts_with("s3://") || html.starts_with("http://") || html.starts_with("https://") {
        return ExtractionResult {
            parse_error: Some(
                "html_extract expects HTML content, not URL. \
                 Use: html_extract(s3_fetch('s3://...')) or html_extract(http_fetch('...'))"
                    .to_string(),
            ),
            ..Default::default()
        };
    }

    let document = Html::parse_document(html);
    let selector = match Selector::parse("h1, h2, h3, h4, h5, h6, p") {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResult {
                parse_error: Some(format!("Selector parse error: {:?}", e)),
                ..Default::default()
            };
        }
    };

    let mut context = HeadingContext::default();
    let mut paragraphs: Vec<(String, HeadingContext, usize)> = Vec::new();
    let mut raw_elements: Vec<RawElement> = Vec::new();
    let mut position = 0;

    for element in document.select(&selector) {
        if is_heading(&element) {
            update_context_from_element(&mut context, &element);

            if matches!(config.mode, ChunkMode::Raw) {
                let text: String = element.text().collect::<String>().trim().to_string();
                let word_count = text.split_whitespace().count();
                let classes = element
                    .value()
                    .attr("class")
                    .unwrap_or("")
                    .to_string();

                raw_elements.push(RawElement {
                    position,
                    element_type: element.value().name().to_string(),
                    text,
                    word_count: word_count as i32,
                    context: context.clone(),
                    classes,
                });
                position += 1;
            }
            continue;
        }

        if is_paragraph(&element) {
            if is_filtered(&element, &config.filters) {
                continue;
            }

            let text: String = element.text().collect::<String>().trim().to_string();
            let word_count = text.split_whitespace().count();

            if matches!(config.mode, ChunkMode::Raw) {
                let classes = element
                    .value()
                    .attr("class")
                    .unwrap_or("")
                    .to_string();

                raw_elements.push(RawElement {
                    position,
                    element_type: "p".to_string(),
                    text,
                    word_count: word_count as i32,
                    context: context.clone(),
                    classes,
                });
                position += 1;
            } else {
                paragraphs.push((text, context.clone(), word_count));
            }
        }
    }

    let chunks = if matches!(config.mode, ChunkMode::Raw) {
        Vec::new()
    } else {
        apply_chunking(paragraphs, config.mode)
    };

    ExtractionResult {
        chunks,
        raw_elements,
        parse_error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_extraction() {
        let html = r#"
            <html>
            <body>
                <h1>Chapter 1</h1>
                <p>This is a paragraph with enough words to pass the filter.</p>
                <h2>Section 1.1</h2>
                <p>Another paragraph with more content to parse and extract from the document.</p>
            </body>
            </html>
        "#;

        let config = ExtractConfig {
            mode: ChunkMode::Paragraph,
            filters: FilterConfig {
                enabled: true,
                min_words: 5,
                ..Default::default()
            },
        };

        let result = extract_and_chunk(html, &config);
        assert!(result.parse_error.is_none());
        assert_eq!(result.chunks.len(), 2);
        assert_eq!(result.chunks[0].context.h1, Some("Chapter 1".to_string()));
    }

    #[test]
    fn test_url_rejection() {
        let result = extract_and_chunk("s3://bucket/file.html", &ExtractConfig::default());
        assert!(result.parse_error.is_some());
        assert!(result.parse_error.unwrap().contains("expects HTML content"));
    }

    use super::super::types::FilterConfig;
}
