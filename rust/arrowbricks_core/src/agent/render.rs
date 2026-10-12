use crate::client::ApiError;
use arrow_array::{Array, LargeStringArray, RecordBatch, StringArray, StringViewArray};
use arrow_cast::display::{ArrayFormatter, ArrayFormatterFactory, DisplayIndex, FormatOptions, FormatResult};
use arrow_schema::{DataType, SchemaRef};
use std::fmt::{self, Write};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Instant;

const FOOTER_RESERVE: usize = 160;
const CLIPPED: &str = "…[truncated]";

/// Text layout of an agent preview.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OutputFormat {
    /// Tab-separated values, `name:Type` header. `\\`, tab, newline and carriage
    /// return are backslash-escaped, SQL NULL is `\N` and the empty string is an
    /// empty field (PostgreSQL `COPY` text conventions), so a cell can never
    /// contain a raw delimiter.
    #[default]
    Tsv,
    /// Pipe table without padding; strings are quoted and Markdown-escaped.
    Markdown,
}

impl OutputFormat {
    pub fn parse(name: &str) -> Result<Self, &'static str> {
        match name {
            "tsv" => Ok(Self::Tsv),
            "markdown" => Ok(Self::Markdown),
            _ => Err("output_format must be 'tsv' or 'markdown'"),
        }
    }
}

pub struct MarkdownRenderer {
    format: OutputFormat,
    schema: SchemaRef,
    output: String,
    scratch: String,
    max_rows: usize,
    max_bytes: usize,
    max_cell_chars: usize,
    rows: usize,
    full: bool,
    omitted_rows: bool,
    cells_truncated: bool,
    interrupt: Option<Interrupt>,
}
impl MarkdownRenderer {
    pub fn new(
        schema: SchemaRef,
        max_rows: usize,
        max_bytes: usize,
        max_cell_chars: usize,
        format: OutputFormat,
    ) -> Result<Self, ApiError> {
        Self::new_inner(schema, max_rows, max_bytes, max_cell_chars, format, None)
    }
    pub fn new_with_interrupt(
        schema: SchemaRef,
        max_rows: usize,
        max_bytes: usize,
        max_cell_chars: usize,
        format: OutputFormat,
        deadline: Instant,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Self, ApiError> {
        Self::new_inner(
            schema,
            max_rows,
            max_bytes,
            max_cell_chars,
            format,
            Some(Interrupt { deadline, cancelled }),
        )
    }
    pub fn set_interrupt(&mut self, deadline: Instant, cancelled: Arc<AtomicBool>) {
        self.interrupt = Some(Interrupt { deadline, cancelled });
    }
    fn new_inner(
        schema: SchemaRef,
        max_rows: usize,
        max_bytes: usize,
        max_cell_chars: usize,
        format: OutputFormat,
        interrupt: Option<Interrupt>,
    ) -> Result<Self, ApiError> {
        if max_bytes < 256 || max_rows == 0 || max_cell_chars == 0 {
            return Err(ApiError::permanent(
                "agent budgets require max_bytes >= 256, max_rows >= 1 and max_cell_chars >= 1",
            ));
        }
        if let Some(control) = &interrupt {
            control.check().map_err(ApiError::permanent)?;
        }
        let mut output = String::with_capacity(max_bytes.min(10_000));
        let mut header = EscapedWriter::new(&mut output, usize::MAX, max_bytes - FOOTER_RESERVE);
        header.tsv = format == OutputFormat::Tsv;
        header.interrupt = interrupt.as_ref();
        let mut full = false;
        if schema.fields().is_empty() {
            header
                .write_str("Result has no columns.")
                .map_err(|_| ApiError::permanent(header.interrupted.unwrap_or("agent schema formatting failed")))?;
        } else {
            let result = (|| -> fmt::Result {
                match format {
                    OutputFormat::Markdown => {
                        header.raw("|")?;
                        for field in schema.fields() {
                            header.write_str(field.name())?;
                            header.raw(" (")?;
                            write!(header, "{}", field.data_type())?;
                            header.raw(")|")?;
                        }
                        header.raw("\n|")?;
                        for _ in schema.fields() {
                            header.raw("-|")?;
                        }
                    }
                    OutputFormat::Tsv => {
                        for (index, field) in schema.fields().iter().enumerate() {
                            if index > 0 {
                                header.raw("\t")?;
                            }
                            header.write_str(field.name())?;
                            header.raw(":")?;
                            write!(header, "{}", field.data_type())?;
                        }
                    }
                }
                header.raw("\n")
            })();
            if let Some(message) = header.interrupted {
                return Err(ApiError::permanent(message));
            }
            if result.is_err() {
                output.clear();
                output.push_str("Result schema exceeds max_bytes; increase max_bytes.\n");
                full = true;
            }
        }
        Ok(Self {
            format,
            schema,
            output,
            scratch: String::new(),
            max_rows,
            max_bytes,
            max_cell_chars,
            rows: 0,
            full,
            omitted_rows: full,
            cells_truncated: false,
            interrupt,
        })
    }
    pub fn rows(&self) -> usize {
        self.rows
    }
    pub fn is_full(&self) -> bool {
        self.full || self.rows == self.max_rows
    }
    pub fn push_batch(&mut self, batch: &RecordBatch) -> Result<bool, ApiError> {
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(ApiError::permanent("agent result schema changed between batches"));
        }
        if self.full {
            self.omitted_rows |= batch.num_rows() != 0;
            return Ok(false);
        }
        let options = FormatOptions::new()
            .with_null("NULL")
            .with_quoted_strings(true)
            .with_formatter_factory(Some(&StreamingStrings));
        let formatters = batch
            .columns()
            .iter()
            .map(|a| ArrayFormatter::try_new(a.as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| ApiError::permanent(e.to_string()))?;
        for row in 0..batch.num_rows() {
            if let Some(control) = &self.interrupt {
                control.check().map_err(ApiError::permanent)?;
            }
            if self.rows == self.max_rows {
                self.omitted_rows = true;
                return Ok(false);
            }
            if batch.num_columns() == 0 {
                self.rows += 1;
                continue;
            }
            self.scratch.clear();
            let available = self.max_bytes - FOOTER_RESERVE - self.output.len();
            let tsv = self.format == OutputFormat::Tsv;
            if !tsv {
                self.scratch.push('|');
            }
            let mut row_clipped = false;
            for (index, (column, formatter)) in batch.columns().iter().zip(&formatters).enumerate() {
                // Reserve closing quote, clipping marker, and delimiter before formatting.
                let reserve = CLIPPED.len() + 5;
                if self.scratch.len().saturating_add(reserve + 2) > available {
                    self.full = true;
                    self.omitted_rows = true;
                    return Ok(false);
                }
                if tsv && index > 0 {
                    self.scratch.push('\t');
                }
                let quote = !tsv
                    && !column.is_null(row)
                    && matches!(
                        column.data_type(),
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    );
                if quote {
                    self.scratch.push('"');
                }
                let cell_bytes = available - self.scratch.len() - reserve;
                let mut sink = EscapedWriter::new(&mut self.scratch, self.max_cell_chars, cell_bytes);
                sink.tsv = tsv;
                sink.interrupt = self.interrupt.as_ref();
                let result = if column.is_null(row) {
                    sink.chars = usize::MAX;
                    sink.bytes = sink.bytes.max(4);
                    // `\N` is the TSV NULL marker; a literal backslash-N string renders as `\\N`.
                    sink.raw(if tsv { "\\N" } else { "NULL" })
                        .map_err(|_| arrow_schema::ArrowError::CastError("bounded output".into()))
                } else if let Some(a) = column.as_any().downcast_ref::<StringArray>() {
                    sink.write_str(a.value(row))
                        .map_err(|_| arrow_schema::ArrowError::CastError("bounded output".into()))
                } else if let Some(a) = column.as_any().downcast_ref::<LargeStringArray>() {
                    sink.write_str(a.value(row))
                        .map_err(|_| arrow_schema::ArrowError::CastError("bounded output".into()))
                } else if let Some(a) = column.as_any().downcast_ref::<StringViewArray>() {
                    sink.write_str(a.value(row))
                        .map_err(|_| arrow_schema::ArrowError::CastError("bounded output".into()))
                } else {
                    formatter.value(row).write(&mut sink)
                };
                if let Some(message) = sink.interrupted {
                    return Err(ApiError::permanent(message));
                }
                let clipped = sink.clipped;
                if let Err(error) = result
                    && !clipped
                {
                    return Err(ApiError::permanent(error.to_string()));
                }
                if clipped {
                    self.scratch.push_str(CLIPPED);
                    row_clipped = true;
                }
                if quote {
                    self.scratch.push('"');
                }
                if !tsv {
                    self.scratch.push('|');
                }
            }
            self.scratch.push('\n');
            if self.scratch.len() > available {
                self.full = true;
                self.omitted_rows = true;
                return Ok(false);
            }
            self.output.push_str(&self.scratch);
            self.cells_truncated |= row_clipped;
            self.rows += 1;
        }
        Ok(!self.is_full())
    }
    pub fn finish(mut self, source_exhausted: bool) -> String {
        let exact = source_exhausted && !self.omitted_rows;
        let complete = exact && !self.cells_truncated;
        write!(
            self.output,
            "\nrows_shown={}; complete={complete}; total_rows{}{}",
            self.rows,
            if exact { "=" } else { ">=" },
            self.rows
        )
        .unwrap();
        if self.cells_truncated {
            self.output.push_str("; cells_truncated=true");
        }
        if !complete {
            self.output.push_str("; next=filter/aggregate SQL or raise budgets");
        }
        debug_assert!(self.output.len() <= self.max_bytes);
        self.output
    }
}

struct Interrupt {
    deadline: Instant,
    cancelled: Arc<AtomicBool>,
}
impl Interrupt {
    fn check(&self) -> Result<(), &'static str> {
        if self.cancelled.load(Ordering::Relaxed) {
            Err("agent rendering cancelled")
        } else if Instant::now() >= self.deadline {
            Err("agent rendering deadline exceeded")
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
struct StreamingStrings;
impl ArrayFormatterFactory for StreamingStrings {
    fn create_array_formatter<'a>(
        &self,
        array: &'a dyn Array,
        options: &FormatOptions<'a>,
        _: Option<&'a arrow_schema::Field>,
    ) -> Result<Option<ArrayFormatter<'a>>, arrow_schema::ArrowError> {
        if matches!(
            array.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            Ok(Some(ArrayFormatter::new(
                Box::new(StreamingString {
                    array,
                    quoted: options.quoted_strings(),
                }),
                false,
            )))
        } else {
            Ok(None)
        }
    }
}
struct StreamingString<'a> {
    array: &'a dyn Array,
    quoted: bool,
}
impl DisplayIndex for StreamingString<'_> {
    fn write(&self, row: usize, writer: &mut dyn Write) -> FormatResult {
        if self.array.is_null(row) {
            writer.write_str("NULL")?;
            return Ok(());
        }
        let value = if let Some(a) = self.array.as_any().downcast_ref::<StringArray>() {
            a.value(row)
        } else if let Some(a) = self.array.as_any().downcast_ref::<LargeStringArray>() {
            a.value(row)
        } else {
            self.array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .value(row)
        };
        if self.quoted {
            writer.write_str("\"")?;
        }
        writer.write_str(value)?;
        if self.quoted {
            writer.write_str("\"")?;
        }
        Ok(())
    }
}

struct EscapedWriter<'a> {
    output: &'a mut String,
    chars: usize,
    bytes: usize,
    clipped: bool,
    interrupt: Option<&'a Interrupt>,
    interrupted: Option<&'static str>,
    until_check: usize,
    tsv: bool,
}
impl<'a> EscapedWriter<'a> {
    fn new(output: &'a mut String, chars: usize, bytes: usize) -> Self {
        Self {
            output,
            chars,
            bytes,
            clipped: false,
            interrupt: None,
            interrupted: None,
            until_check: 0,
            tsv: false,
        }
    }
    fn raw(&mut self, text: &str) -> fmt::Result {
        if text.len() > self.bytes {
            self.clipped = true;
            return Err(fmt::Error);
        }
        self.bytes -= text.len();
        self.output.push_str(text);
        Ok(())
    }
}
impl Write for EscapedWriter<'_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        for ch in text.chars() {
            if self.until_check == 0 {
                if let Some(control) = self.interrupt
                    && let Err(message) = control.check()
                {
                    self.interrupted = Some(message);
                    return Err(fmt::Error);
                }
                self.until_check = 256;
            }
            self.until_check -= 1;
            if self.chars == 0 {
                self.clipped = true;
                return Err(fmt::Error);
            }
            match ch {
                // TSV needs no Markdown/HTML escaping, only delimiter and control-character safety.
                '&' | '<' | '>' | '|' | '`' | '*' | '_' | '[' | ']' | '~' | '"' if self.tsv => {
                    self.raw(ch.encode_utf8(&mut [0; 4]))?
                }
                '&' => self.raw("&amp;")?,
                '<' => self.raw("&lt;")?,
                '>' => self.raw("&gt;")?,
                '\n' => self.raw("\\n")?,
                '\r' => self.raw("\\r")?,
                '\t' => self.raw("\\t")?,
                '\\' | '|' | '`' | '*' | '_' | '[' | ']' | '~' | '"' => {
                    self.raw("\\")?;
                    self.raw(ch.encode_utf8(&mut [0; 4]))?;
                }
                c if c.is_control() => {
                    let mut escaped = [0_u8; 10];
                    let mut at = escaped.len();
                    let mut code = c as u32;
                    while code != 0 || escaped.len() - at < 4 {
                        at -= 1;
                        escaped[at] = b"0123456789abcdef"[(code & 15) as usize];
                        code >>= 4;
                    }
                    at -= 2;
                    escaped[at..at + 2].copy_from_slice(b"\\u");
                    self.raw(std::str::from_utf8(&escaped[at..]).unwrap())?;
                }
                c => self.raw(c.encode_utf8(&mut [0; 4]))?,
            }
            self.chars -= 1;
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::Int32Type;
    use arrow_array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array, ListArray,
        StringArray, StructArray, TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(names: &[&str], arrays: Vec<ArrayRef>) -> RecordBatch {
        let fields: Vec<_> = names
            .iter()
            .zip(&arrays)
            .map(|(name, a)| Field::new(*name, a.data_type().clone(), true))
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }
    fn render(batch: &RecordBatch, rows: usize, bytes: usize, chars: usize, exhausted: bool) -> String {
        let mut r = MarkdownRenderer::new(batch.schema(), rows, bytes, chars, OutputFormat::Markdown).unwrap();
        r.push_batch(batch).unwrap();
        r.finish(exhausted)
    }
    #[test]
    fn zero_column_batches_count_rows_without_malformed_table() {
        let b = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap();
        let out = render(&b, 10, 256, 500, true);
        assert!(
            out.contains("total_rows=3") && out.contains("complete=true") && !out.contains('|'),
            "{out}"
        );
    }
    #[test]
    fn control_escaping_matches_hex_format_for_every_control_character() {
        for code in 0..=0x10ffff {
            if let Some(ch) = char::from_u32(code)
                && ch.is_control()
            {
                let mut output = String::new();
                EscapedWriter::new(&mut output, 1, 64)
                    .write_str(ch.encode_utf8(&mut [0; 4]))
                    .unwrap();
                let expected = match ch {
                    '\t' => "\\t".to_owned(),
                    '\n' => "\\n".to_owned(),
                    '\r' => "\\r".to_owned(),
                    _ => format!("\\u{code:04x}"),
                };
                assert_eq!(output, expected);
            }
        }
    }
    #[test]
    fn rendering_interrupts_are_errors_not_clipped_success() {
        let b = batch(&["a"], vec![Arc::new(StringArray::from(vec!["long value"]))]);
        let flag = Arc::new(AtomicBool::new(false));
        assert!(
            MarkdownRenderer::new_with_interrupt(
                b.schema(),
                10,
                1000,
                500,
                OutputFormat::Markdown,
                Instant::now(),
                flag.clone()
            )
            .is_err()
        );
        let mut r = MarkdownRenderer::new(b.schema(), 10, 1000, 500, OutputFormat::Markdown).unwrap();
        r.set_interrupt(Instant::now() + std::time::Duration::from_secs(10), flag.clone());
        flag.store(true, Ordering::Relaxed);
        assert!(r.push_batch(&b).unwrap_err().message.contains("cancelled"));
        let control = Interrupt {
            deadline: Instant::now() + std::time::Duration::from_secs(10),
            cancelled: flag,
        };
        let mut output = String::new();
        let mut sink = EscapedWriter::new(&mut output, 1000, 5000);
        sink.interrupt = Some(&control);
        sink.until_check = 256;
        assert!(sink.write_str(&"a".repeat(1000)).is_err());
        assert_eq!(sink.interrupted, Some("agent rendering cancelled"));
        assert!(!sink.clipped);
        assert_eq!(output.len(), 256);
    }
    proptest::proptest! {
        #[test]
        fn arbitrary_unicode_cells_obey_whole_output_cap(value in ".{0,100}", bytes in 256usize..1000, chars in 1usize..100, columns in 1usize..8) {
            let arrays = (0..columns).map(|_| Arc::new(StringArray::from(vec![value.clone(); 4])) as ArrayRef).collect();
            let names = vec!["c"; columns];
            let b = batch(&names, arrays);
            let out = render(&b, 4, bytes, chars, true);
            proptest::prop_assert!(out.len() <= bytes);
            proptest::prop_assert!(out.contains("complete="));
        }
    }
    #[test]
    fn exact_count_survives_cell_clipping_and_null_is_never_clipped() {
        let b = batch(&["s"], vec![Arc::new(StringArray::from(vec![Some("long"), None]))]);
        let out = render(&b, 10, 1000, 1, true);
        assert!(
            out.contains("total_rows=2") && out.contains("complete=false") && out.contains("cells_truncated=true"),
            "{out}"
        );
        assert!(out.contains("|NULL|"), "{out}");
        assert!(out.contains("next=filter/aggregate SQL or raise budgets"));
    }
    #[test]
    fn large_and_view_strings_dictionary_null_and_invalid_budgets() {
        use arrow_array::types::Int8Type;
        use arrow_array::{DictionaryArray, LargeStringArray, NullArray, StringViewArray};
        let dict: DictionaryArray<Int8Type> = vec![Some("NULL"), None].into_iter().collect();
        let b = batch(
            &["large", "view", "dict", "null"],
            vec![
                Arc::new(LargeStringArray::from(vec!["NULL", "é"])),
                Arc::new(StringViewArray::from(vec!["NULL", "🦀"])),
                Arc::new(dict),
                Arc::new(NullArray::new(2)),
            ],
        );
        let out = render(&b, 10, 2000, 500, true);
        assert!(out.contains("complete=true") && out.contains("\"NULL\""), "{out}");
        for (rows, bytes, chars) in [(0, 256, 1), (1, 255, 1), (1, 256, 0)] {
            assert!(MarkdownRenderer::new(b.schema(), rows, bytes, chars, OutputFormat::Markdown).is_err());
        }
    }
    #[test]
    fn nested_huge_strings_are_explicitly_clipped() {
        use arrow_array::builder::{ListBuilder, StringBuilder};
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value("🦀".repeat(1_000_000));
        builder.append(true);
        let b = batch(&["list"], vec![Arc::new(builder.finish())]);
        let out = render(&b, 10, 512, 20, true);
        assert!(
            out.len() <= 512 && out.contains("…[truncated]") && out.contains("total_rows=1"),
            "{out}"
        );
    }
    #[test]
    fn typed_header_null_and_duplicate_names() {
        let b = batch(
            &["x", "x"],
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None])),
                Arc::new(StringArray::from(vec![Some("NULL"), None])),
            ],
        );
        let out = render(&b, 10, 1000, 500, true);
        assert!(out.contains("x (Int64)") && out.contains("x (Utf8)"), "{out}");
        assert!(out.contains("|1|\"NULL\"|"), "{out}");
        assert!(out.contains("|NULL|NULL|"), "{out}");
        assert!(out.contains("complete=true") && out.contains("total_rows=2"), "{out}");
    }
    #[test]
    fn malicious_markdown_and_unicode_are_escaped() {
        let b = batch(
            &["<x>|\\"],
            vec![Arc::new(StringArray::from(vec!["é🦀|\\\n<script>&*_`[]"]))],
        );
        let out = render(&b, 10, 1000, 500, true);
        assert!(out.contains("é🦀\\|\\\\\\n&lt;script&gt;&amp;\\*\\_\\`\\[\\]"), "{out}");
        assert!(!out.contains("<script>") && !out.contains("<x>"));
    }
    #[test]
    fn row_limit_and_cell_clipping_are_honest() {
        let b = batch(&["s"], vec![Arc::new(StringArray::from(vec!["é🦀abcd", "other"]))]);
        let out = render(&b, 1, 1000, 3, true);
        assert!(out.contains("é🦀a…[truncated]"), "{out}");
        assert!(out.contains("rows_shown=1") && out.contains("complete=false"), "{out}");
        assert!(!out.contains("total_rows=1"));
    }
    #[test]
    fn every_budget_and_wide_schema_stays_bounded() {
        let b = batch(
            &["a", "b"],
            vec![
                Arc::new(StringArray::from(vec!["|<🦀".repeat(200); 10])),
                Arc::new(Int64Array::from(vec![42; 10])),
            ],
        );
        for bytes in 256..600 {
            let out = render(&b, 10, bytes, 500, true);
            assert!(out.len() <= bytes, "{} > {bytes}", out.len());
            assert!(out.contains("complete=false"));
            for line in out.lines().filter(|line| line.starts_with('|')) {
                assert!(line.ends_with('|'), "{line}");
            }
        }
        let b = batch(&[&"long".repeat(1000)], vec![Arc::new(Int64Array::from(vec![1]))]);
        let out = render(&b, 10, 256, 500, true);
        assert!(
            out.len() <= 256 && out.contains("schema") && out.contains("complete=false"),
            "{out}"
        );
        assert!(!out.contains('|'));
    }
    #[test]
    fn empty_batches_keep_schema_and_exact_zero() {
        let b = batch(&["a"], vec![Arc::new(Int64Array::from(Vec::<i64>::new()))]);
        let out = render(&b, 10, 1000, 500, true);
        assert!(
            out.contains("a (Int64)") && out.contains("total_rows=0") && out.contains("complete=true"),
            "{out}"
        );
    }
    #[test]
    fn scalar_nested_decimal_and_temporal_types() {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(1), None])]);
        let fields = vec![Arc::new(Field::new("s", DataType::Utf8, true))].into();
        let structure = StructArray::new(fields, vec![Arc::new(StringArray::from(vec!["inner|<x>"]))], None);
        let b = batch(
            &["bool", "float", "decimal", "date", "time", "binary", "list", "struct"],
            vec![
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(Float64Array::from(vec![f64::INFINITY])),
                Arc::new(
                    Decimal128Array::from(vec![12345])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
                Arc::new(Date32Array::from(vec![0])),
                Arc::new(TimestampMicrosecondArray::from(vec![0])),
                Arc::new(BinaryArray::from_vec(vec![b"ab"])),
                Arc::new(list),
                Arc::new(structure),
            ],
        );
        let out = render(&b, 10, 4000, 500, true);
        for value in [
            "true",
            "inf",
            "123.45",
            "1970-01-01",
            "6162",
            "[1,",
            "inner\\|&lt;x&gt;",
        ] {
            assert!(out.contains(value), "missing {value}: {out}");
        }
    }
    fn render_tsv(batch: &RecordBatch, rows: usize, bytes: usize, chars: usize) -> String {
        let mut r = MarkdownRenderer::new(batch.schema(), rows, bytes, chars, OutputFormat::Tsv).unwrap();
        r.push_batch(batch).unwrap();
        r.finish(true)
    }
    /// Reference decoder for the documented TSV cell grammar.
    fn tsv_cell(raw: &str) -> Option<String> {
        if raw == "\\N" {
            return None;
        }
        let mut out = String::new();
        let mut chars = raw.chars();
        while let Some(c) = chars.next() {
            if c != '\\' {
                out.push(c);
                continue;
            }
            match chars.next().unwrap() {
                '\\' => out.push('\\'),
                't' => out.push('\t'),
                'n' => out.push('\n'),
                'r' => out.push('\r'),
                'u' => {
                    let hex: String = chars.by_ref().take(4).collect();
                    out.push(char::from_u32(u32::from_str_radix(&hex, 16).unwrap()).unwrap());
                }
                other => panic!("unexpected escape \\{other}"),
            }
        }
        Some(out)
    }
    #[test]
    fn tsv_is_the_default_layout_with_typed_header() {
        assert_eq!(OutputFormat::default(), OutputFormat::Tsv);
        let b = batch(
            &["id", "label"],
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a_b"), None])),
            ],
        );
        let out = render_tsv(&b, 10, 1000, 500);
        assert_eq!(
            out,
            "id:Int64\tlabel:Utf8\n1\ta_b\n2\t\\N\n\nrows_shown=2; complete=true; total_rows=2"
        );
    }
    #[test]
    fn tsv_tabs_newlines_backslashes_and_markers_cannot_break_the_table() {
        let nasty = [
            "plain",
            "a\tb",
            "tab at end\t",
            "\tleading tab",
            "line1\nline2",
            "crlf\r\n",
            "back\\slash",
            "ends with backslash\\",
            "\\t is not a tab",
            "\\N",
            "NULL",
            "",
            " padded ",
            "pipe|and,comma\"quote\"",
            "bell\u{7} nul\u{0} nel\u{85}",
            "Zürich 東京 🚀",
            "\t\t\t",
        ];
        let values: Vec<Option<&str>> = nasty.iter().map(|v| Some(*v)).chain([None]).collect();
        let b = batch(
            &["k", "v", "w"],
            vec![
                Arc::new(Int64Array::from_iter_values(0..values.len() as i64)),
                Arc::new(StringArray::from(values.clone())),
                Arc::new(StringArray::from(values.clone())),
            ],
        );
        let out = render_tsv(&b, 100, 10_000, 500);
        let (table, footer) = out.split_once("\n\nrows_shown=").unwrap();
        assert!(footer.contains("complete=true"), "{footer}");
        let lines: Vec<&str> = table.lines().collect();
        assert_eq!(lines.len(), values.len() + 1, "a cell leaked a line break: {out:?}");
        for (line, expected) in lines[1..].iter().zip(&values) {
            let cells: Vec<&str> = line.split('\t').collect();
            assert_eq!(cells.len(), 3, "a cell leaked a tab: {line:?}");
            for cell in &cells[1..] {
                assert_eq!(tsv_cell(cell).as_deref(), *expected, "{line:?}");
            }
        }
    }
    #[test]
    fn tsv_string_null_marker_and_empty_string_are_distinct() {
        let b = batch(
            &["s"],
            vec![Arc::new(StringArray::from(vec![
                Some("\\N"),
                None,
                Some(""),
                Some("NULL"),
            ]))],
        );
        let out = render_tsv(&b, 10, 1000, 500);
        assert!(out.contains("\n\\\\N\n\\N\n\nNULL\n"), "{out:?}");
    }
    #[test]
    fn tsv_clips_cells_and_never_exceeds_the_byte_budget() {
        let b = batch(
            &["a", "b"],
            vec![
                Arc::new(StringArray::from(vec!["\t🦀\n".repeat(300); 10])),
                Arc::new(Int64Array::from(vec![42; 10])),
            ],
        );
        for bytes in 256..700 {
            let out = render_tsv(&b, 10, bytes, 500);
            assert!(out.len() <= bytes, "{} > {bytes}", out.len());
            assert!(out.contains("complete=false"));
            for line in out.lines().take_while(|l| !l.is_empty()) {
                assert_eq!(line.matches('\t').count(), 1, "{line:?}");
            }
        }
        let out = render_tsv(&b, 10, 4000, 3);
        assert!(
            out.contains("\\t🦀\\n…[truncated]\t42") || out.contains("…[truncated]\t42"),
            "{out:?}"
        );
        assert!(out.contains("cells_truncated=true"));
    }
    #[test]
    fn tsv_nested_values_and_header_names_are_escaped_too() {
        use arrow_array::builder::{ListBuilder, StringBuilder};
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value("x\ty\nz");
        builder.append(true);
        let b = batch(
            &["we\tird\nname", "list"],
            vec![Arc::new(Int64Array::from(vec![1])), Arc::new(builder.finish())],
        );
        let out = render_tsv(&b, 10, 1000, 500);
        let lines: Vec<&str> = out.lines().take_while(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 2, "{out:?}");
        assert!(lines[0].starts_with("we\\tird\\nname:Int64\tlist:"), "{out:?}");
        assert_eq!(lines[1].matches('\t').count(), 1, "{out:?}");
        assert!(lines[1].contains("x\\ty\\nz"), "{out:?}");
    }
    proptest::proptest! {
        #[test]
        fn tsv_roundtrips_arbitrary_strings(values in proptest::collection::vec(proptest::option::of("\\PC{0,12}|[\t\n\r\\\\N|]{0,6}"), 1..8)) {
            let b = batch(&["a", "b"], vec![
                Arc::new(StringArray::from(values.clone())),
                Arc::new(StringArray::from(values.clone())),
            ]);
            let out = render_tsv(&b, 100, 100_000, 500);
            let table = out.split_once("\n\nrows_shown=").unwrap().0;
            let lines: Vec<&str> = table.split('\n').collect();
            proptest::prop_assert_eq!(lines.len(), values.len() + 1);
            for (line, expected) in lines[1..].iter().zip(&values) {
                let cells: Vec<&str> = line.split('\t').collect();
                proptest::prop_assert_eq!(cells.len(), 2);
                for cell in cells {
                    proptest::prop_assert_eq!(tsv_cell(cell), expected.clone());
                }
            }
        }
    }
    #[test]
    fn tsv_round_trips_every_unicode_scalar_value() {
        let all: Vec<char> = (0..=0x10ffff_u32).filter_map(char::from_u32).collect();
        let chunks: Vec<String> = all.chunks(512).map(|c| c.iter().collect()).collect();
        let b = batch(&["v"], vec![Arc::new(StringArray::from(chunks.clone()))]);
        let mut r = MarkdownRenderer::new(b.schema(), chunks.len(), 64 << 20, 1 << 20, OutputFormat::Tsv).unwrap();
        r.push_batch(&b).unwrap();
        let out = r.finish(true);
        let table = out.split_once("\n\nrows_shown=").unwrap().0;
        let lines: Vec<&str> = table.split('\n').collect();
        assert_eq!(lines.len(), chunks.len() + 1, "a character leaked a line break");
        for (line, expected) in lines[1..].iter().zip(&chunks) {
            assert!(!line.contains('\t'), "a character leaked a tab");
            assert_eq!(tsv_cell(line).as_deref(), Some(expected.as_str()));
        }
    }
    #[test]
    fn markdown_has_no_padding_and_a_minimal_separator() {
        let b = batch(
            &["a", "b"],
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        );
        let out = render(&b, 10, 1000, 500, true);
        assert!(out.starts_with("|a (Int64)|b (Utf8)|\n|-|-|\n|1|\"x\"|\n"), "{out}");
    }
    #[test]
    fn output_format_names() {
        assert_eq!(OutputFormat::parse("tsv"), Ok(OutputFormat::Tsv));
        assert_eq!(OutputFormat::parse("markdown"), Ok(OutputFormat::Markdown));
        assert!(OutputFormat::parse("csv").is_err());
    }
    #[test]
    fn batch_schema_mismatch_errors_and_multiple_batches_append() {
        let b = batch(&["a"], vec![Arc::new(Int64Array::from(vec![1, 2]))]);
        let mut r = MarkdownRenderer::new(b.schema(), 3, 1000, 500, OutputFormat::Markdown).unwrap();
        assert!(r.push_batch(&b).unwrap());
        assert!(!r.push_batch(&b).unwrap());
        assert_eq!(r.rows(), 3);
        assert!(r.is_full());
        let out = r.finish(false);
        assert!(out.contains("rows_shown=3") && !out.contains("total_rows="));
        let other = batch(&["b"], vec![Arc::new(Int64Array::from(vec![1]))]);
        let mut r = MarkdownRenderer::new(b.schema(), 3, 1000, 500, OutputFormat::Markdown).unwrap();
        assert!(r.push_batch(&other).is_err());
    }
}
