use scraper::ElementRef;
use super::types::HeadingContext;

pub fn update_context_from_element(context: &mut HeadingContext, element: &ElementRef) {
    let tag_name = element.value().name();
    if let Some(level_char) = tag_name.chars().nth(1) {
        if let Some(level) = level_char.to_digit(10) {
            let text: String = element.text().collect::<String>().trim().to_string();
            context.update(level as u8, text);
        }
    }
}

pub fn is_heading(element: &ElementRef) -> bool {
    matches!(
        element.value().name(),
        "h1" | "h2" | "h3" | "h4" | "h5" | "h6"
    )
}

pub fn is_paragraph(element: &ElementRef) -> bool {
    element.value().name() == "p"
}
