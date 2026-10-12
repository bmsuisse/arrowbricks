use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, Float64Array, Int64Array, ListArray, RecordBatch, StringArray};
use arrow_schema::{Field, Schema};
use arrowbricks_core::agent::render::{AgentRenderer, OutputFormat};
use std::{hint::black_box, sync::Arc, time::Instant};

fn batch(columns: Vec<ArrayRef>) -> RecordBatch {
    let fields: Vec<_> = columns
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), true))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}
fn direct(b: &RecordBatch, bytes: usize, interrupts: bool) -> String {
    let mut renderer = if interrupts {
        AgentRenderer::new_with_interrupt(
            b.schema(),
            1000,
            bytes,
            500,
            OutputFormat::Markdown,
            Instant::now() + std::time::Duration::from_secs(60),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
    } else {
        AgentRenderer::new(b.schema(), 1000, bytes, 500, OutputFormat::Markdown)
    }
    .unwrap();
    renderer.push_batch(b).unwrap();
    renderer.finish(true)
}
fn json(b: &RecordBatch) -> Vec<serde_json::Value> {
    let mut writer = arrow_json::ArrayWriter::new(Vec::new());
    writer.write_batches(&[b]).unwrap();
    writer.finish().unwrap();
    serde_json::from_slice(&writer.into_inner()).unwrap()
}
fn json_markdown(b: &RecordBatch, bytes: usize) -> String {
    let values = json(b);
    let mut output = String::new();
    for row in &values {
        let mut line = String::from("|");
        for field in b.schema().fields() {
            let value = &row[field.name()];
            let owned;
            let text = if let Some(value) = value.as_str() {
                value
            } else {
                owned = value.to_string();
                &owned
            };
            line.push(' ');
            if value.is_string() {
                line.push('"');
            }
            for ch in text.chars().take(500) {
                match ch {
                    '&' => line.push_str("&amp;"),
                    '<' => line.push_str("&lt;"),
                    '>' => line.push_str("&gt;"),
                    '\n' => line.push_str("\\n"),
                    '\r' => line.push_str("\\r"),
                    '\t' => line.push_str("\\t"),
                    '\\' | '|' | '`' | '*' | '_' | '[' | ']' | '~' | '"' => {
                        line.push('\\');
                        line.push(ch);
                    }
                    c => line.push(c),
                }
            }
            if value.is_string() {
                line.push('"');
            }
            line.push_str(" |");
        }
        line.push('\n');
        if output.len() + line.len() > bytes.saturating_sub(160) {
            break;
        }
        output.push_str(&line);
    }
    output
}
fn time(mut f: impl FnMut(), loops: usize) -> f64 {
    let start = Instant::now();
    for _ in 0..loops {
        f();
    }
    start.elapsed().as_secs_f64() * 1e6 / loops as f64
}
fn main() {
    let agent_only = std::env::args().any(|arg| arg == "--agent-only");
    let n = 1000;
    let numeric = batch(vec![
        Arc::new(Int64Array::from_iter_values(0..n)),
        Arc::new(Float64Array::from_iter_values((0..n).map(|i| i as f64 * 0.1))),
    ]);
    let unicode = batch(vec![
        Arc::new(StringArray::from(vec!["é🦀|<>&\\".repeat(100); n as usize])),
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            (0..n).map(|_| Some(vec![Some(1), None, Some(3)])),
        )),
    ]);
    let ascii = batch(vec![Arc::new(StringArray::from(vec![
        "ordinary query results without markup ".repeat(20); n as usize
    ]))]);
    let controls = batch(vec![Arc::new(StringArray::from(vec![
        "a\0\u{0001}b\t\n".repeat(30);
        n as usize
    ]))]);
    let huge = batch(vec![Arc::new(StringArray::from(vec!["🦀".repeat(1_000_000)]))]);
    let mut builder = arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new());
    builder.values().append_value("🦀".repeat(2_500_000));
    builder.append(true);
    let nested_huge = batch(vec![Arc::new(builder.finish())]);
    for (name, b, loops) in [
        ("numeric", numeric, 1000),
        ("unicode_nested", unicode, 100),
        ("ascii_strings", ascii, 100),
        ("controls", controls, 100),
        ("4MB_string", huge, 20),
        ("10MB_nested_string", nested_huge, 10),
    ] {
        let bounded = time(
            || {
                black_box(direct(&b, 10_000, agent_only));
            },
            if agent_only { 10000 } else { loops },
        );
        let full = time(
            || {
                black_box(direct(&b, 2_000_000, agent_only));
            },
            loops,
        );
        if agent_only {
            println!("{name}: direct_10KB={bounded:.2}us direct_2MB={full:.2}us");
            continue;
        }
        let json_render = time(
            || {
                black_box(json_markdown(&b, 10_000));
            },
            loops,
        );
        let via_json = time(
            || {
                black_box(json(&b));
            },
            loops,
        );
        println!(
            "{name}: direct_10KB={bounded:.2}us direct_2MB={full:.2}us JSON_materialize={via_json:.2}us JSON_then_Markdown_10KB={json_render:.2}us"
        );
    }
}
