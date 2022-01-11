use anyhow::Result;
use rustyline::completion::{Completer, Pair};
use rustyline::config::OutputStreamType;
use rustyline::error::ReadlineError;
use rustyline::highlight::{Highlighter, MatchingBracketHighlighter};
use rustyline::hint::{Hinter, HistoryHinter};
use rustyline::validate::{self, MatchingBracketValidator, Validator};
use rustyline::{CompletionType, Config, Context, EditMode, Editor};
use rustyline_derive::Helper;
use scylla::transport::Compression;
use scylla::{QueryResult, Session, SessionBuilder};
use std::borrow::Cow::{self, Borrowed, Owned};
use std::env;

#[derive(Helper)]
struct CqlHelper {
    highlighter: MatchingBracketHighlighter,
    validator: MatchingBracketValidator,
    hinter: HistoryHinter,
    colored_prompt: String,
}

impl Completer for CqlHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> Result<(usize, Vec<Pair>), ReadlineError> {
        let start: usize = line[..pos].rfind(' ').unwrap_or(0);
        if line.len() > 0 {
            println!("found {}", line[start..pos].to_owned());
        }
        Ok((
            1,
            vec![Pair {
                display: "x".to_owned(),
                replacement: "y".to_owned(),
            }],
        ))
    }
}

impl Hinter for CqlHelper {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, ctx: &Context<'_>) -> Option<String> {
        self.hinter.hint(line, pos, ctx)
    }
}

impl Highlighter for CqlHelper {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> Cow<'b, str> {
        if default {
            Borrowed(&self.colored_prompt)
        } else {
            Borrowed(prompt)
        }
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Owned("\x1b[1m".to_owned() + hint + "\x1b[m")
    }

    fn highlight<'l>(&self, line: &'l str, pos: usize) -> Cow<'l, str> {
        self.highlighter.highlight(line, pos)
    }

    fn highlight_char(&self, line: &str, pos: usize) -> bool {
        self.highlighter.highlight_char(line, pos)
    }
}

impl Validator for CqlHelper {
    fn validate(
        &self,
        ctx: &mut validate::ValidationContext,
    ) -> rustyline::Result<validate::ValidationResult> {
        self.validator.validate(ctx)
    }

    fn validate_while_typing(&self) -> bool {
        self.validator.validate_while_typing()
    }
}

fn print_result(result: &QueryResult) {
    if result.rows.is_none() {
        println!("OK");
        return;
    }
    for row in result.rows.as_ref().unwrap() {
        for column in &row.columns {
            print!("|");
            print!(
                " {:16}",
                match column {
                    None => "null".to_owned(),
                    Some(value) => format!("{:?}", value),
                }
            );
        }
        println!("|")
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .compression(Some(Compression::Lz4))
        .build()
        .await?;

    let config = Config::builder()
        .history_ignore_space(true)
        .completion_type(CompletionType::List)
        .edit_mode(EditMode::Emacs)
        .output_stream(OutputStreamType::Stdout)
        .build();
    let mut rl = Editor::with_config(config);
    let helper = CqlHelper {
        highlighter: MatchingBracketHighlighter::new(),
        hinter: HistoryHinter {},
        colored_prompt: "".to_owned(),
        validator: MatchingBracketValidator::new(),
    };
    rl.set_helper(Some(helper));
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                if line.is_empty() {
                    continue;
                }
                rl.add_history_entry(line.as_str());
                let maybe_res = session.query(line, &[]).await;
                match maybe_res {
                    Err(err) => println!("Error: {}", err),
                    Ok(res) => print_result(&res),
                }
            }
            Err(ReadlineError::Interrupted) => continue,
            Err(ReadlineError::Eof) => break,
            Err(err) => println!("Error: {}", err),
        }
    }
    Ok(())
}
