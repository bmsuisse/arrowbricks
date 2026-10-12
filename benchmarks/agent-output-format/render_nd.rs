use arrow_json::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema};
use arrowbricks_core::agent::render::{MarkdownRenderer, OutputFormat};
use std::{fs::File, io::BufReader, str::FromStr, sync::Arc};
// usage: render_nd <data.ndjson> <schema.tsv: name<TAB>ArrowType per line> <tsv|markdown>
fn main() {
    let a: Vec<String> = std::env::args().collect();
    let fields: Vec<Field> = std::fs::read_to_string(&a[2]).unwrap().lines().map(|l| {
        let (n, t) = l.split_once('\t').unwrap();
        Field::new(n, DataType::from_str(t).unwrap_or_else(|e| panic!("{t}: {e}")), true)
    }).collect();
    let schema = Arc::new(Schema::new(fields));
    let reader = ReaderBuilder::new(schema.clone()).with_batch_size(4096).build(BufReader::new(File::open(&a[1]).unwrap())).unwrap();
    let format = if a[3] == "tsv" { OutputFormat::Tsv } else { OutputFormat::Markdown };
    let mut r = MarkdownRenderer::new(schema, 100000, 50_000_000, 500, format).unwrap();
    for b in reader { r.push_batch(&b.unwrap()).unwrap(); }
    print!("{}", r.finish(true));
}
