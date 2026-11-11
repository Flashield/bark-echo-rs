//! README seam: every command the README shows must exist in the binary's
//! actual CLI (ticket #8: "every command shown works as written"). The
//! test parses the README's fenced command lines and backticked
//! `bark-echo …` invocations, resolves each to a subcommand (and, under
//! `subscribe`, a subcommand of it), and checks the command names against
//! real `--help` output and every `--flag` against that subcommand's own
//! help — so a rename without a README update, or the reverse, fails
//! here. Placeholders (`URL`, `KEY`, `<command>`) are skipped as values;
//! flags are always checked literally.

use std::collections::BTreeMap;
use std::process::Command;

/// The README at the repo root, one level above this crate.
const README: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../README.md");

fn run_help(args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_bark-echo"))
        .args(args)
        .arg("--help")
        .output()
        .expect("spawn bark-echo for --help");
    assert!(
        output.status.success(),
        "--help must exit 0 for {args:?}, got {output:?}"
    );
    String::from_utf8(output.stdout).expect("utf-8 help output")
}

/// The `Commands:` section's names from a clap help text: the lines
/// between `Commands:` and `Options:`, each `  <name>  <description>`.
fn command_names(help: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut in_commands = false;
    for line in help.lines() {
        match line.trim_end() {
            "Commands:" => in_commands = true,
            "Options:" => in_commands = false,
            _ if in_commands => {
                if let Some(name) = line.split_whitespace().next() {
                    names.push(name.to_owned());
                }
            }
            _ => {}
        }
    }
    names
}

/// One invocation found in the README: the resolved subcommand path
/// (e.g. `["subscribe", "add"]`) and the flags written on it.
struct Invocation {
    path: Vec<String>,
    flags: Vec<String>,
}

/// Classify the tokens after `bark-echo`: skip placeholders and flag
/// values, collect the command path (top-level, then the `subscribe`
/// leaf) and every `--flag`.
fn parse_invocation(tokens: &[String]) -> Invocation {
    let is_command_word = |token: &str| {
        !token.is_empty()
            && token.chars().all(|c| c.is_ascii_lowercase() || c == '-')
            && token.chars().any(|c| c.is_ascii_lowercase())
    };
    let mut path = Vec::new();
    let mut flags = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        let token = tokens[index].as_str();
        if token.starts_with("--") {
            flags.push(token.to_owned());
            // A flag's value rides along — unless the next token is
            // itself a flag (the flag was valueless).
            if tokens
                .get(index + 1)
                .is_some_and(|next| !next.starts_with('-'))
            {
                index += 1;
            }
        } else if (path.is_empty() || path.last().map(String::as_str) == Some("subscribe"))
            && is_command_word(token)
        {
            // A lowercase word is a command name candidate; placeholders
            // (URL, KEY, <command>, …) never match this shape.
            path.push(token.to_owned());
        }
        index += 1;
    }
    Invocation { path, flags }
}

/// The README's invocations: fenced command lines (shell continuations
/// joined, `$` prompts and comments dropped) plus every backtick span
/// that starts with `bark-echo ` (the cheat-sheet table).
fn readme_invocations() -> Vec<String> {
    let readme = std::fs::read_to_string(README).expect("README.md at the repo root");

    let mut lines = Vec::new();
    let mut in_fence = false;
    let mut joined = String::new();
    for line in readme.lines() {
        if line.trim_start().starts_with("```") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence {
            // Join shell line continuations first, then keep only the
            // binary's own invocations as README claims about the CLI
            // (other tools' commands — cargo … — and comments are not).
            let line = line.trim();
            joined.push_str(line.strip_prefix("$ ").unwrap_or(line));
            joined.push(' ');
            if joined.trim_end().ends_with('\\') {
                let taken = std::mem::take(&mut joined);
                joined.push_str(taken.trim_end().trim_end_matches('\\'));
                joined.push(' ');
                continue;
            }
            let full = std::mem::take(&mut joined);
            let full = full.trim();
            if full.starts_with('#') || full.is_empty() {
                continue;
            }
            if let Some(rest) = full.strip_prefix("bark-echo ") {
                lines.push(rest.to_owned());
            }
        } else {
            // Prose and tables: only explicit `bark-echo …` spans count.
            // `split('`')` alternates outside/inside spans; odd indices
            // are the insides.
            for span in line.split('`').skip(1).step_by(2) {
                let span = span.trim();
                if let Some(rest) = span.strip_prefix("bark-echo ") {
                    lines.push(rest.to_owned());
                }
            }
        }
    }
    lines
}

/// Strip the `[option]` syntax's brackets and trailing punctuation so
/// `[--tail N]` yields the flag `--tail` and the placeholder `N`.
fn clean_token(token: &str) -> String {
    token
        .trim_start_matches('[')
        .trim_end_matches([']', ',', '.'])
        .to_owned()
}

#[test]
fn every_command_and_flag_the_readme_shows_exists_in_the_cli() {
    let top_help = run_help(&[]);
    let subscribe_help = run_help(&["subscribe"]);
    let top_commands = command_names(&top_help);
    let subscribe_commands = command_names(&subscribe_help);
    assert!(
        top_commands.contains(&"subscribe".to_owned()),
        "help parsing broke: {top_commands:?}"
    );

    // One --help text per resolved command path, fetched lazily.
    let mut help_cache: BTreeMap<Vec<String>, String> = BTreeMap::new();
    let mut checked = 0;
    for line in readme_invocations() {
        let tokens: Vec<String> = line.split_whitespace().map(clean_token).collect();
        let invocation = parse_invocation(&tokens);
        if invocation.path.first().is_some_and(|name| {
            ["help", "--help", "-h", "--version", "-V"].contains(&name.as_str())
        }) {
            continue;
        }

        let path: Vec<&str> = invocation.path.iter().map(String::as_str).collect();
        let (universe, help_args) = match path.as_slice() {
            [] => {
                // A flag-only span (e.g. the global `--plain-secrets`
                // row): its flags must live in the top-level help.
                (top_commands.as_slice(), vec![])
            }
            ["subscribe"] => (subscribe_commands.as_slice(), vec!["subscribe"]),
            ["subscribe", leaf] => (
                // The leaf's own help is fetched below; existence is
                // checked against `subscribe --help`.
                subscribe_commands.as_slice(),
                vec!["subscribe", *leaf],
            ),
            [command] => {
                assert!(
                    top_commands.contains(&command.to_string()),
                    "README shows `bark-echo {line}` but `{command}` is not a command (top-level: {top_commands:?})"
                );
                (top_commands.as_slice(), vec![*command])
            }
            other => panic!("README shows an unresolvable command path {other:?}: `{line}`"),
        };

        if let [.., leaf] = path.as_slice()
            && path.len() >= 2
        {
            assert!(
                universe.contains(&leaf.to_string()),
                "README shows `bark-echo {line}` but `{leaf}` is not a subcommand of its parent (have: {universe:?})"
            );
        }

        if !invocation.flags.is_empty() {
            let help = help_cache
                .entry(invocation.path.clone())
                .or_insert_with(|| run_help(&help_args));
            for flag in &invocation.flags {
                assert!(
                    help.contains(flag.as_str()),
                    "README shows `{flag}` on `bark-echo {line}` but it is absent from `bark-echo {} --help`:\n{help}",
                    invocation.path.join(" ")
                );
            }
        }
        checked += 1;
    }
    assert!(
        checked >= 10,
        "README parsing found too few invocations ({checked}) — the parser or the README broke"
    );
}
