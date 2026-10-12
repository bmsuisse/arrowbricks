# Reading task

Folders X, Y and Z each hold ten text files (`orders.txt`, `events.txt`, ...). Each file is a SQL query result printed as text; the layout differs between folders. The header gives column names and their types. A footer line says how many rows are shown.

`questions.json` lists questions, each tagged with a dataset (the file name without `.txt`).

## Rules
- Use ONLY the Read tool (and Write for your answers file). No shell, no code execution, no grep/awk/wc, no scripts: read the text yourself.
- Read only the folder you were assigned, plus `questions.json`. Do not open any other folder or file in the project.
- Answer from the text in front of you. For long files read them in chunks if needed. If you cannot determine an answer, say "UNKNOWN".

## Answer format
- Answer with the ORIGINAL value as stored in the database, undoing any escaping the printed layout uses (for example if a layout writes a backslash before an underscore to mark it, give the plain underscore).
- SQL NULL -> `NULL`. Empty string -> `EMPTY`. Booleans -> `true` / `false`.
- Numbers as plain digits (no thousands separators). Dates/timestamps exactly as printed.
- Strings: exact original text. Write a real tab as `\t`, a real newline as `\n`, a real carriage return as `\r`, and a real backslash as `\\`.
- Counting and "largest" questions: count/scan carefully across all rows of that dataset.
- For questions that ask you to choose among words (e.g. 'string' or 'null'), answer with exactly one of those words.
