use super::types::{Chunk, ChunkMode, HeadingContext};

struct Paragraph {
    text: String,
    context: HeadingContext,
    word_count: usize,
}

pub fn apply_chunking(
    paragraphs: Vec<(String, HeadingContext, usize)>,
    mode: ChunkMode,
) -> Vec<Chunk> {
    match mode {
        ChunkMode::Paragraph => paragraph_chunks(paragraphs),
        ChunkMode::Semantic { min_words, max_words } => {
            semantic_chunks(paragraphs, min_words, max_words)
        }
        ChunkMode::Raw => {
            Vec::new()
        }
    }
}

fn paragraph_chunks(paragraphs: Vec<(String, HeadingContext, usize)>) -> Vec<Chunk> {
    paragraphs
        .into_iter()
        .enumerate()
        .map(|(idx, (text, context, word_count))| Chunk {
            chunk_id: idx as i32,
            text,
            context,
            word_count: word_count as i32,
            paragraph_count: 1,
            error: None,
        })
        .collect()
}

fn semantic_chunks(
    paragraphs: Vec<(String, HeadingContext, usize)>,
    min_words: usize,
    max_words: usize,
) -> Vec<Chunk> {
    let mut chunks = Vec::new();
    let mut current_text = String::new();
    let mut current_word_count = 0;
    let mut current_paragraph_count = 0;
    let mut current_context = HeadingContext::default();
    let mut chunk_id = 0;

    for (text, context, word_count) in paragraphs {
        if current_word_count + word_count > max_words && current_word_count >= min_words {
            chunks.push(Chunk {
                chunk_id,
                text: current_text.trim().to_string(),
                context: current_context.clone(),
                word_count: current_word_count as i32,
                paragraph_count: current_paragraph_count,
                error: None,
            });
            chunk_id += 1;
            current_text = text;
            current_word_count = word_count;
            current_paragraph_count = 1;
            current_context = context;
        } else {
            if !current_text.is_empty() {
                current_text.push_str("\n\n");
            }
            current_text.push_str(&text);
            current_word_count += word_count;
            current_paragraph_count += 1;
            if current_paragraph_count == 1 {
                current_context = context;
            }
        }
    }

    if !current_text.is_empty() {
        chunks.push(Chunk {
            chunk_id,
            text: current_text.trim().to_string(),
            context: current_context,
            word_count: current_word_count as i32,
            paragraph_count: current_paragraph_count,
            error: None,
        });
    }

    chunks
}
