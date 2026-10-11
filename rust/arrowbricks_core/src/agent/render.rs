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

pub struct MarkdownRenderer {
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
    pub fn new(schema: SchemaRef, max_rows: usize, max_bytes: usize, max_cell_chars: usize) -> Result<Self, ApiError> {
        Self::new_inner(schema, max_rows, max_bytes, max_cell_chars, None)
    }
    pub fn new_with_interrupt(
        schema: SchemaRef,
        max_rows: usize,
        max_bytes: usize,
        max_cell_chars: usize,
        deadline: Instant,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Self, ApiError> {
        Self::new_inner(
            schema,
            max_rows,
            max_bytes,
            max_cell_chars,
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
        header.interrupt = interrupt.as_ref();
        let mut full = false;
        if schema.fields().is_empty() {
            header
                .write_str("Result has no columns.")
                .map_err(|_| ApiError::permanent(header.interrupted.unwrap_or("agent schema formatting failed")))?;
        } else {
            let result = (|| -> fmt::Result {
                header.raw("| ")?;
                for field in schema.fields() {
                    header.write_str(field.name())?;
                    header.raw(" (")?;
                    write!(header, "{}", field.data_type())?;
                    header.raw(") | ")?;
                }
                header.output.pop();
                header.bytes += 1;
                header.raw("\n|")?;
                for _ in schema.fields() {
                    header.raw(" --- |")?;
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
            self.scratch.push('|');
            let mut row_clipped = false;
            for (column, formatter) in batch.columns().iter().zip(&formatters) {
                // Reserve closing quote, clipping marker, and delimiter before formatting.
                let reserve = CLIPPED.len() + 5;
                if self.scratch.len().saturating_add(reserve + 2) > available {
                    self.full = true;
                    self.omitted_rows = true;
                    return Ok(false);
                }
                self.scratch.push(' ');
                let is_null = column.is_null(row);
                let quote = !is_null
                    && matches!(
                        column.data_type(),
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    );
                if quote {
                    self.scratch.push('"');
                }
                let cell_bytes = available - self.scratch.len() - reserve;
                let mut sink = EscapedWriter::new(&mut self.scratch, self.max_cell_chars, cell_bytes);
                sink.interrupt = self.interrupt.as_ref();
                let result = if is_null {
                    sink.chars = usize::MAX;
                    sink.bytes = sink.bytes.max(4);
                    sink.write_str("NULL")
                        .map_err(|_| arrow_schema::ArrowError::CastError("bounded output".into()))
                } else if quote {
                    let value = if let Some(a) = column.as_any().downcast_ref::<StringArray>() {
                        a.value(row)
                    } else if let Some(a) = column.as_any().downcast_ref::<LargeStringArray>() {
                        a.value(row)
                    } else {
                        column.as_any().downcast_ref::<StringViewArray>().unwrap().value(row)
                    };
                    sink.write_str(value)
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
                self.scratch.push_str(" |");
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
                    let mut escaped = String::new();
                    write!(escaped, "\\u{:04x}", c as u32)?;
                    self.raw(&escaped)?;
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
        let mut r = MarkdownRenderer::new(batch.schema(), rows, bytes, chars).unwrap();
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
    fn rendering_interrupts_are_errors_not_clipped_success() {
        let b = batch(&["a"], vec![Arc::new(StringArray::from(vec!["long value"]))]);
        let flag = Arc::new(AtomicBool::new(false));
        assert!(MarkdownRenderer::new_with_interrupt(b.schema(), 10, 1000, 500, Instant::now(), flag.clone()).is_err());
        let mut r = MarkdownRenderer::new(b.schema(), 10, 1000, 500).unwrap();
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
        assert!(out.contains("| NULL |"), "{out}");
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
            assert!(MarkdownRenderer::new(b.schema(), rows, bytes, chars).is_err());
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
        assert!(out.contains("| 1 | \"NULL\" |"), "{out}");
        assert!(out.contains("| NULL | NULL |"), "{out}");
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
    #[test]
    fn batch_schema_mismatch_errors_and_multiple_batches_append() {
        let b = batch(&["a"], vec![Arc::new(Int64Array::from(vec![1, 2]))]);
        let mut r = MarkdownRenderer::new(b.schema(), 3, 1000, 500).unwrap();
        assert!(r.push_batch(&b).unwrap());
        assert!(!r.push_batch(&b).unwrap());
        assert_eq!(r.rows(), 3);
        assert!(r.is_full());
        let out = r.finish(false);
        assert!(out.contains("rows_shown=3") && !out.contains("total_rows="));
        let other = batch(&["b"], vec![Arc::new(Int64Array::from(vec![1]))]);
        let mut r = MarkdownRenderer::new(b.schema(), 3, 1000, 500).unwrap();
        assert!(r.push_batch(&other).is_err());
    }
}
