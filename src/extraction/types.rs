use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ChunkMode {
    Semantic { min_words: usize, max_words: usize },
    Paragraph,
    Raw,
}

impl Default for ChunkMode {
    fn default() -> Self {
        ChunkMode::Semantic {
            min_words: 100,
            max_words: 500,
        }
    }
}

impl ChunkMode {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "semantic" | "rag" => ChunkMode::default(),
            "paragraph" | "nlp" => ChunkMode::Paragraph,
            "raw" | "elements" => ChunkMode::Raw,
            _ => ChunkMode::default(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct HeadingContext {
    pub h1: Option<String>,
    pub h2: Option<String>,
    pub h3: Option<String>,
    pub h4: Option<String>,
    pub h5: Option<String>,
    pub h6: Option<String>,
}

impl HeadingContext {
    pub fn update(&mut self, level: u8, text: String) {
        match level {
            1 => {
                self.h1 = Some(text);
                self.h2 = None;
                self.h3 = None;
                self.h4 = None;
                self.h5 = None;
                self.h6 = None;
            }
            2 => {
                self.h2 = Some(text);
                self.h3 = None;
                self.h4 = None;
                self.h5 = None;
                self.h6 = None;
            }
            3 => {
                self.h3 = Some(text);
                self.h4 = None;
                self.h5 = None;
                self.h6 = None;
            }
            4 => {
                self.h4 = Some(text);
                self.h5 = None;
                self.h6 = None;
            }
            5 => {
                self.h5 = Some(text);
                self.h6 = None;
            }
            6 => {
                self.h6 = Some(text);
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone)]
pub struct Chunk {
    pub chunk_id: i32,
    pub text: String,
    pub context: HeadingContext,
    pub word_count: i32,
    pub paragraph_count: i32,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RawElement {
    pub position: i32,
    pub element_type: String,
    pub text: String,
    pub word_count: i32,
    pub context: HeadingContext,
    pub classes: String,
}

#[derive(Debug, Clone)]
pub struct ExtractionResult {
    pub chunks: Vec<Chunk>,
    pub raw_elements: Vec<RawElement>,
    pub parse_error: Option<String>,
    pub element_count: usize,
    pub filtered_count: usize,
}

impl Default for ExtractionResult {
    fn default() -> Self {
        Self {
            chunks: Vec::new(),
            raw_elements: Vec::new(),
            parse_error: None,
            element_count: 0,
            filtered_count: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FilterConfig {
    pub enabled: bool,
    pub skip_classes: Vec<String>,
    pub min_words: usize,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            skip_classes: vec![
                "nav".into(),
                "navbar".into(),
                "navigation".into(),
                "menu".into(),
                "sidebar".into(),
                "sidenav".into(),
                "footer".into(),
                "copyright".into(),
                "breadcrumb".into(),
                "header".into(),
                "advertisement".into(),
            ],
            min_words: 10,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExtractConfig {
    pub mode: ChunkMode,
    pub filters: FilterConfig,
}

impl Default for ExtractConfig {
    fn default() -> Self {
        Self {
            mode: ChunkMode::default(),
            filters: FilterConfig::default(),
        }
    }
}

impl ExtractConfig {
    pub fn from_json(json: &str) -> Result<Self, Box<dyn Error>> {
        let value: serde_json::Value = serde_json::from_str(json)?;

        let mode = value
            .get("mode")
            .and_then(|v| v.as_str())
            .map(ChunkMode::from_str)
            .unwrap_or_default();

        let mode = match mode {
            ChunkMode::Semantic { .. } => {
                let min_words = value
                    .get("min_words")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(100) as usize;
                let max_words = value
                    .get("max_words")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(500) as usize;
                ChunkMode::Semantic { min_words, max_words }
            }
            other => other,
        };

        let filters = if let Some(filter_obj) = value.get("filters") {
            FilterConfig {
                enabled: filter_obj
                    .get("enabled")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true),
                skip_classes: filter_obj
                    .get("skip_classes")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_else(|| FilterConfig::default().skip_classes),
                min_words: filter_obj
                    .get("min_words")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(10) as usize,
            }
        } else {
            FilterConfig::default()
        };

        Ok(Self { mode, filters })
    }
}

#[derive(Debug)]
pub enum ExtractionErrorKind {
    Parse,
    InputTooLarge,
    InvalidUtf8,
    Network,
}

#[derive(Debug)]
pub struct ExtractionError {
    pub kind: ExtractionErrorKind,
    pub message: String,
    pub recoverable: bool,
}

impl fmt::Display for ExtractionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for ExtractionError {}
