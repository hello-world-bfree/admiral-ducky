use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_html_parsing(c: &mut Criterion) {
    let html = r#"
        <!DOCTYPE html>
        <html>
        <head><title>Test</title></head>
        <body>
            <h1>Chapter 1</h1>
            <p>This is a paragraph with some text content for benchmarking.</p>
            <h2>Section 1.1</h2>
            <p>Another paragraph with more content to parse and extract.</p>
            <p>Yet another paragraph to increase the workload slightly.</p>
        </body>
        </html>
    "#;

    c.bench_function("scraper_parse_small", |b| {
        b.iter(|| {
            let document = scraper::Html::parse_document(black_box(html));
            let selector = scraper::Selector::parse("p").unwrap();
            let _paragraphs: Vec<_> = document.select(&selector).collect();
        })
    });
}

criterion_group!(benches, bench_html_parsing);
criterion_main!(benches);
