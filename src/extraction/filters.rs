use scraper::ElementRef;
use super::types::FilterConfig;

pub fn is_filtered(element: &ElementRef, config: &FilterConfig) -> bool {
    if !config.enabled {
        return false;
    }

    if let Some(classes) = element.value().attr("class") {
        for class in classes.split_whitespace() {
            if config
                .skip_classes
                .iter()
                .any(|skip| class.to_lowercase().contains(&skip.to_lowercase()))
            {
                return true;
            }
        }
    }

    let text: String = element.text().collect();
    let word_count = text.split_whitespace().count();
    word_count < config.min_words
}
