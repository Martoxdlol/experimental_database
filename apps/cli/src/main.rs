//! exdb-cli — command-line client for embedded databases and JSON wire servers.
//!
//! The CLI intentionally exposes two small, explicit modes:
//! - embedded mode opens one durable database path and runs local operations;
//! - server mode sends one JSON text protocol message to a running TCP server.

use std::error::Error;
use std::fmt;
use std::io::{BufRead, Write};
use std::net::SocketAddr;
use std::path::PathBuf;

use exdb::{
    Database, DatabaseConfig, FieldPath, Filter, RangeExpr, Scalar, ScanDirection,
    TransactionOptions, TransactionResult, decode_ulid, encode_ulid,
};
use exdb_wire::{RawFrame, read_frame_with_limit, write_frame_with_limit};
use serde_json::{Value, json};
use tokio::net::TcpStream;

const DEFAULT_DATA_PATH: &str = "./data/default";
const DEFAULT_DATABASE: &str = "default";

#[tokio::main]
async fn main() {
    let mut stdout = std::io::stdout();
    if let Err(err) = run(std::env::args().skip(1), &mut stdout).await {
        eprintln!("exdb-cli: {err}");
        std::process::exit(1);
    }
}

async fn run(
    args: impl IntoIterator<Item = String>,
    out: &mut dyn Write,
) -> std::result::Result<(), CliError> {
    let invocation = Invocation::parse(args)?;
    if invocation.help {
        writeln!(out, "{}", help_text())?;
        return Ok(());
    }

    if matches!(invocation.command, Command::Repl) {
        let stdin = std::io::stdin();
        let mut input = stdin.lock();
        run_repl(
            invocation.mode,
            invocation.database,
            invocation.pretty,
            &mut input,
            out,
        )
        .await?;
        return Ok(());
    }

    let output =
        execute_command(&invocation.mode, &invocation.database, invocation.command).await?;
    write_json(out, &output, invocation.pretty)?;
    Ok(())
}

fn help_text() -> &'static str {
    "exdb-cli 0.1.0

Usage:
  exdb-cli [--data-path <PATH>] [--pretty] <COMMAND> [ARGS]
  exdb-cli --connect <ADDR> [--database <NAME>] [--pretty] <COMMAND> [ARGS]
  exdb-cli --connect <ADDR> [--pretty] send-json <JSON>
  exdb-cli [--connect <ADDR>] [--database <NAME>] [--pretty] repl

Embedded commands:
  repl
  list-collections
  create-collection <name>
  drop-collection <name>
  list-indexes <collection>
  create-index <collection> <index> <field> [field...]
  drop-index <collection> <index>
  insert <collection> <json-document>
  get <collection> <doc-id>
  replace <collection> <doc-id> <json-document>
  patch <collection> <doc-id> <json-merge-patch>
  delete <collection> <doc-id>
  query <collection> <index> [--range <JSON>] [--filter <JSON>] [--order asc|desc] [--limit N]
  check-integrity

Server-only commands:
  list-databases
  create-database <name>
  drop-database <name>

Options:
  --data-path <PATH>    Embedded database path [default: ./data/default]
  --connect <ADDR>     TCP server address for JSON text protocol mode
  --database <NAME>    Server database for collection/document commands [default: default]
  --pretty             Pretty-print JSON output
  -h, --help           Show this help

Examples:
  exdb-cli --data-path ./data/app create-collection users
  exdb-cli --data-path ./data/app insert users '{\"name\":\"Ada\"}'
  exdb-cli --connect 127.0.0.1:5200 --database app insert users '{\"name\":\"Ada\"}'
  exdb-cli --connect 127.0.0.1:5200 --database app repl
  exdb-cli --connect 127.0.0.1:5200 send-json '{\"id\":1,\"type\":\"ping\"}'"
}

#[derive(Debug)]
struct Invocation {
    mode: Mode,
    command: Command,
    database: String,
    pretty: bool,
    help: bool,
}

#[derive(Debug, Clone)]
enum Mode {
    Embedded { data_path: PathBuf },
    Server { connect: SocketAddr },
}

#[derive(Debug)]
enum Command {
    Repl,
    ListDatabases,
    CreateDatabase {
        name: String,
    },
    DropDatabase {
        name: String,
    },
    ListCollections,
    CreateCollection {
        name: String,
    },
    DropCollection {
        name: String,
    },
    ListIndexes {
        collection: String,
    },
    CreateIndex {
        collection: String,
        name: String,
        fields: Vec<FieldPath>,
    },
    DropIndex {
        collection: String,
        name: String,
    },
    Insert {
        collection: String,
        body: Value,
    },
    Get {
        collection: String,
        doc_id: String,
    },
    Replace {
        collection: String,
        doc_id: String,
        body: Value,
    },
    Patch {
        collection: String,
        doc_id: String,
        patch: Value,
    },
    Delete {
        collection: String,
        doc_id: String,
    },
    Query {
        collection: String,
        index: String,
        range: Vec<RangeExpr>,
        filter: Option<Filter>,
        order: Option<ScanDirection>,
        limit: Option<usize>,
    },
    CheckIntegrity,
    SendJson {
        message: Value,
    },
}

impl Invocation {
    fn parse(args: impl IntoIterator<Item = String>) -> std::result::Result<Self, CliError> {
        let mut data_path = PathBuf::from(DEFAULT_DATA_PATH);
        let mut database = DEFAULT_DATABASE.to_string();
        let mut connect = None;
        let mut pretty = false;
        let mut help = false;
        let mut rest = Vec::new();

        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-h" | "--help" => help = true,
                "--pretty" => pretty = true,
                "--data-path" => data_path = next_path(&mut args, "--data-path")?,
                "--database" => database = next_string(&mut args, "--database")?,
                "--connect" => connect = Some(next_addr(&mut args, "--connect")?),
                _ => {
                    rest.push(arg);
                    rest.extend(args);
                    break;
                }
            }
        }

        if help {
            return Ok(Self {
                mode: Mode::Embedded { data_path },
                command: Command::ListCollections,
                database,
                pretty,
                help,
            });
        }

        let command = if rest.is_empty() {
            Command::Repl
        } else {
            Command::parse(rest)?
        };
        let mode = match connect {
            Some(connect) => Mode::Server { connect },
            None => Mode::Embedded { data_path },
        };
        validate_command_for_mode(&mode, &command)?;

        Ok(Self {
            mode,
            command,
            database,
            pretty,
            help,
        })
    }
}

impl Command {
    fn parse(args: Vec<String>) -> std::result::Result<Self, CliError> {
        let mut args = args.into_iter();
        let Some(command) = args.next() else {
            return Err(CliError::Usage("missing command".to_string()));
        };

        let parsed = match command.as_str() {
            "repl" => {
                expect_end(args)?;
                Command::Repl
            }
            "list-databases" => {
                expect_end(args)?;
                Command::ListDatabases
            }
            "create-database" => {
                let name = next_string(&mut args, "create-database <name>")?;
                expect_end(args)?;
                Command::CreateDatabase { name }
            }
            "drop-database" => {
                let name = next_string(&mut args, "drop-database <name>")?;
                expect_end(args)?;
                Command::DropDatabase { name }
            }
            "list-collections" => {
                expect_end(args)?;
                Command::ListCollections
            }
            "create-collection" => {
                let name = next_string(&mut args, "create-collection <name>")?;
                expect_end(args)?;
                Command::CreateCollection { name }
            }
            "drop-collection" => {
                let name = next_string(&mut args, "drop-collection <name>")?;
                expect_end(args)?;
                Command::DropCollection { name }
            }
            "list-indexes" => {
                let collection = next_string(&mut args, "list-indexes <collection>")?;
                expect_end(args)?;
                Command::ListIndexes { collection }
            }
            "create-index" => {
                let collection = next_string(&mut args, "create-index <collection> <index>")?;
                let name = next_string(&mut args, "create-index <collection> <index>")?;
                let fields = args
                    .map(|field| parse_field_path_string(&field))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                if fields.is_empty() {
                    return Err(CliError::Usage(
                        "create-index requires at least one field".to_string(),
                    ));
                }
                Command::CreateIndex {
                    collection,
                    name,
                    fields,
                }
            }
            "drop-index" => {
                let collection = next_string(&mut args, "drop-index <collection> <index>")?;
                let name = next_string(&mut args, "drop-index <collection> <index>")?;
                expect_end(args)?;
                Command::DropIndex { collection, name }
            }
            "insert" => {
                let collection = next_string(&mut args, "insert <collection> <json-document>")?;
                let body = parse_json_arg(&next_string(
                    &mut args,
                    "insert <collection> <json-document>",
                )?)?;
                expect_end(args)?;
                Command::Insert { collection, body }
            }
            "get" => {
                let collection = next_string(&mut args, "get <collection> <doc-id>")?;
                let doc_id = next_string(&mut args, "get <collection> <doc-id>")?;
                expect_end(args)?;
                Command::Get { collection, doc_id }
            }
            "replace" => {
                let collection =
                    next_string(&mut args, "replace <collection> <doc-id> <json-document>")?;
                let doc_id =
                    next_string(&mut args, "replace <collection> <doc-id> <json-document>")?;
                let body = parse_json_arg(&next_string(
                    &mut args,
                    "replace <collection> <doc-id> <json-document>",
                )?)?;
                expect_end(args)?;
                Command::Replace {
                    collection,
                    doc_id,
                    body,
                }
            }
            "patch" => {
                let collection =
                    next_string(&mut args, "patch <collection> <doc-id> <json-merge-patch>")?;
                let doc_id =
                    next_string(&mut args, "patch <collection> <doc-id> <json-merge-patch>")?;
                let patch = parse_json_arg(&next_string(
                    &mut args,
                    "patch <collection> <doc-id> <json-merge-patch>",
                )?)?;
                expect_end(args)?;
                Command::Patch {
                    collection,
                    doc_id,
                    patch,
                }
            }
            "delete" => {
                let collection = next_string(&mut args, "delete <collection> <doc-id>")?;
                let doc_id = next_string(&mut args, "delete <collection> <doc-id>")?;
                expect_end(args)?;
                Command::Delete { collection, doc_id }
            }
            "query" => parse_query(args)?,
            "check-integrity" => {
                expect_end(args)?;
                Command::CheckIntegrity
            }
            "send-json" => {
                let message = parse_json_arg(&next_string(&mut args, "send-json <JSON>")?)?;
                expect_end(args)?;
                Command::SendJson { message }
            }
            other => return Err(CliError::Usage(format!("unknown command: {other}"))),
        };
        Ok(parsed)
    }
}

fn parse_query(mut args: impl Iterator<Item = String>) -> std::result::Result<Command, CliError> {
    let collection = next_string(&mut args, "query <collection> <index>")?;
    let index = next_string(&mut args, "query <collection> <index>")?;
    let mut range = Vec::new();
    let mut filter = None;
    let mut order = None;
    let mut limit = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--range" => {
                let value = parse_json_arg(&next_string(&mut args, "--range <JSON>")?)?;
                let Value::Array(values) = value else {
                    return Err(CliError::Usage("--range expects a JSON array".to_string()));
                };
                range = parse_range_exprs(values)?;
            }
            "--filter" => {
                let value = parse_json_arg(&next_string(&mut args, "--filter <JSON>")?)?;
                filter = Some(parse_filter(value)?);
            }
            "--order" => {
                let value = next_string(&mut args, "--order asc|desc")?;
                order = parse_order(Some(&value))?;
            }
            "--limit" => {
                let value = next_string(&mut args, "--limit N")?;
                limit = Some(value.parse().map_err(|e| {
                    CliError::Usage(format!("--limit expects an unsigned integer: {e}"))
                })?);
            }
            other => return Err(CliError::Usage(format!("unknown query option: {other}"))),
        }
    }

    Ok(Command::Query {
        collection,
        index,
        range,
        filter,
        order,
        limit,
    })
}

fn validate_command_for_mode(mode: &Mode, command: &Command) -> std::result::Result<(), CliError> {
    if matches!(mode, Mode::Embedded { .. }) && matches!(command, Command::SendJson { .. }) {
        return Err(CliError::Usage(
            "send-json requires --connect <ADDR>".to_string(),
        ));
    }
    if matches!(mode, Mode::Embedded { .. })
        && matches!(
            command,
            Command::ListDatabases | Command::CreateDatabase { .. } | Command::DropDatabase { .. }
        )
    {
        return Err(CliError::Usage(
            "database management commands require --connect <ADDR>".to_string(),
        ));
    }
    Ok(())
}

async fn execute_command(
    mode: &Mode,
    database: &str,
    command: Command,
) -> std::result::Result<Value, CliError> {
    validate_command_for_mode(mode, &command)?;
    match mode {
        Mode::Embedded { data_path } => run_embedded(data_path.clone(), command).await,
        Mode::Server { connect } => run_server(*connect, database, command).await,
    }
}

async fn run_repl(
    mode: Mode,
    database: String,
    pretty: bool,
    input: &mut dyn BufRead,
    out: &mut dyn Write,
) -> std::result::Result<(), CliError> {
    writeln!(
        out,
        "{}",
        json!({
            "ok": true,
            "mode": mode.label(),
            "database": database,
            "commands": ["help", "exit", "quit"]
        })
    )?;

    loop {
        write!(out, "exdb> ")?;
        out.flush()?;

        let mut line = String::new();
        if input.read_line(&mut line)? == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if matches!(trimmed, "exit" | "quit" | ".exit" | ".quit") {
            break;
        }
        if matches!(trimmed, "help" | ".help") {
            writeln!(out, "{}", help_text())?;
            continue;
        }

        match parse_repl_line(trimmed)
            .and_then(Command::parse)
            .and_then(|command| {
                if matches!(command, Command::Repl) {
                    Err(CliError::Usage("already in repl".to_string()))
                } else {
                    validate_command_for_mode(&mode, &command).map(|_| command)
                }
            }) {
            Ok(command) => match execute_command(&mode, &database, command).await {
                Ok(output) => write_json(out, &output, pretty)?,
                Err(err) => write_json(out, &json!({ "error": err.to_string() }), pretty)?,
            },
            Err(err) => write_json(out, &json!({ "error": err.to_string() }), pretty)?,
        }
    }

    Ok(())
}

impl Mode {
    fn label(&self) -> &'static str {
        match self {
            Mode::Embedded { .. } => "embedded",
            Mode::Server { .. } => "server",
        }
    }
}

fn parse_repl_line(line: &str) -> std::result::Result<Vec<String>, CliError> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;

    for ch in line.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }

        match (quote, ch) {
            (_, '\\') => escaped = true,
            (Some(active), ch) if ch == active => quote = None,
            (Some(_), ch) => current.push(ch),
            (None, '\'' | '"') => quote = Some(ch),
            (None, ch) if ch.is_whitespace() => {
                if !current.is_empty() {
                    args.push(std::mem::take(&mut current));
                }
            }
            (None, ch) => current.push(ch),
        }
    }

    if escaped {
        return Err(CliError::Usage(
            "line ends with dangling escape".to_string(),
        ));
    }
    if let Some(quote) = quote {
        return Err(CliError::Usage(format!("unterminated {quote} quote")));
    }
    if !current.is_empty() {
        args.push(current);
    }
    Ok(args)
}

async fn run_embedded(path: PathBuf, command: Command) -> std::result::Result<Value, CliError> {
    let db = Database::open(path, DatabaseConfig::default(), None).await?;
    let output = match command {
        Command::Repl => unreachable!("repl command handled before execution"),
        Command::ListDatabases | Command::CreateDatabase { .. } | Command::DropDatabase { .. } => {
            unreachable!("database management commands rejected during parse")
        }
        Command::ListCollections => {
            let mut tx = db.begin(TransactionOptions::readonly())?;
            let collections = tx
                .list_collections()?
                .into_iter()
                .map(collection_json)
                .collect::<Vec<_>>();
            tx.rollback();
            json!({ "collections": collections })
        }
        Command::CreateCollection { name } => {
            let mut tx = db.begin(TransactionOptions::default())?;
            tx.create_collection(&name).await?;
            commit_json(tx.commit().await?)?
        }
        Command::DropCollection { name } => {
            let mut tx = db.begin(TransactionOptions::default())?;
            tx.drop_collection(&name).await?;
            commit_json(tx.commit().await?)?
        }
        Command::ListIndexes { collection } => {
            let mut tx = db.begin(TransactionOptions::readonly())?;
            let indexes = tx
                .list_indexes(&collection)?
                .into_iter()
                .map(index_json)
                .collect::<Vec<_>>();
            tx.rollback();
            json!({ "indexes": indexes })
        }
        Command::CreateIndex {
            collection,
            name,
            fields,
        } => {
            let mut tx = db.begin(TransactionOptions::default())?;
            tx.create_index(&collection, &name, fields).await?;
            commit_json(tx.commit().await?)?
        }
        Command::DropIndex { collection, name } => {
            let mut tx = db.begin(TransactionOptions::default())?;
            tx.drop_index(&collection, &name).await?;
            commit_json(tx.commit().await?)?
        }
        Command::Insert { collection, body } => {
            let mut tx = db.begin(TransactionOptions::default())?;
            let doc_id = tx.insert(&collection, body).await?;
            let id = encode_ulid(&doc_id);
            let mut output = expect_commit_success(tx.commit().await?)?;
            output["id"] = Value::from(id);
            output
        }
        Command::Get { collection, doc_id } => {
            let doc_id = decode_doc_id(&doc_id)?;
            let mut tx = db.begin(TransactionOptions::readonly())?;
            let (query_id, document) = tx.get_with_query_id(&collection, &doc_id).await?;
            tx.rollback();
            json!({ "query_id": query_id, "document": document })
        }
        Command::Replace {
            collection,
            doc_id,
            body,
        } => {
            let doc_id = decode_doc_id(&doc_id)?;
            let mut tx = db.begin(TransactionOptions::default())?;
            tx.replace(&collection, &doc_id, body).await?;
            commit_json(tx.commit().await?)?
        }
        Command::Patch {
            collection,
            doc_id,
            patch,
        } => {
            let doc_id = decode_doc_id(&doc_id)?;
            let mut tx = db.begin(TransactionOptions::default())?;
            tx.patch(&collection, &doc_id, patch).await?;
            commit_json(tx.commit().await?)?
        }
        Command::Delete { collection, doc_id } => {
            let doc_id = decode_doc_id(&doc_id)?;
            let mut tx = db.begin(TransactionOptions::default())?;
            tx.delete(&collection, &doc_id).await?;
            commit_json(tx.commit().await?)?
        }
        Command::Query {
            collection,
            index,
            range,
            filter,
            order,
            limit,
        } => {
            let mut tx = db.begin(TransactionOptions::readonly())?;
            let (query_id, documents) = tx
                .query_with_query_id(&collection, &index, &range, filter, order, limit)
                .await?;
            tx.rollback();
            json!({ "query_id": query_id, "documents": documents })
        }
        Command::CheckIntegrity => {
            let report = db.check_integrity().await?;
            integrity_json(report)
        }
        Command::SendJson { .. } => unreachable!("server command rejected during parse"),
    };
    db.close().await?;
    Ok(output)
}

async fn run_server(
    addr: SocketAddr,
    database: &str,
    command: Command,
) -> std::result::Result<Value, CliError> {
    let mut stream = TcpStream::connect(addr).await?;
    let hello = read_frame_with_limit(&mut stream, exdb_wire::DEFAULT_MAX_MESSAGE_SIZE).await?;
    let mut client = JsonServerClient::new(stream);

    match command {
        Command::Repl => unreachable!("repl command handled before execution"),
        Command::SendJson { message } => {
            let request = RawFrame::json_text(serde_json::to_vec(&message)?);
            write_frame_with_limit(
                client.stream_mut(),
                &request,
                exdb_wire::DEFAULT_MAX_MESSAGE_SIZE,
            )
            .await?;
            let response =
                read_frame_with_limit(client.stream_mut(), exdb_wire::DEFAULT_MAX_MESSAGE_SIZE)
                    .await?;
            Ok(json!({
                "hello": json_payload(&hello)?,
                "response": json_payload(&response)?
            }))
        }
        Command::ListDatabases => {
            let fields = client.send_ok(json!({ "type": "list_databases" })).await?;
            Ok(Value::Object(fields))
        }
        Command::CreateDatabase { name } => {
            let fields = client
                .send_ok(json!({
                    "type": "create_database",
                    "name": name
                }))
                .await?;
            Ok(ok_output(fields))
        }
        Command::DropDatabase { name } => {
            let fields = client
                .send_ok(json!({
                    "type": "drop_database",
                    "name": name
                }))
                .await?;
            Ok(ok_output(fields))
        }
        Command::ListCollections => {
            let fields = client
                .send_ok(json!({
                    "type": "list_collections",
                    "database": database
                }))
                .await?;
            Ok(Value::Object(fields))
        }
        Command::CreateCollection { name } => {
            let fields = client
                .send_ok(json!({
                    "type": "create_collection",
                    "database": database,
                    "name": name
                }))
                .await?;
            Ok(ok_output(fields))
        }
        Command::DropCollection { name } => {
            let fields = client
                .send_ok(json!({
                    "type": "drop_collection",
                    "database": database,
                    "name": name
                }))
                .await?;
            Ok(ok_output(fields))
        }
        Command::ListIndexes { collection } => {
            let fields = client
                .send_ok(json!({
                    "type": "list_indexes",
                    "database": database,
                    "collection": collection
                }))
                .await?;
            Ok(Value::Object(fields))
        }
        Command::CreateIndex {
            collection,
            name,
            fields,
        } => {
            let fields = client
                .send_ok(json!({
                    "type": "create_index",
                    "database": database,
                    "collection": collection,
                    "name": name,
                    "fields": fields.iter().map(field_path_json).collect::<Vec<_>>()
                }))
                .await?;
            Ok(ok_output(fields))
        }
        Command::DropIndex { collection, name } => {
            let fields = client
                .send_ok(json!({
                    "type": "drop_index",
                    "database": database,
                    "collection": collection,
                    "name": name
                }))
                .await?;
            Ok(ok_output(fields))
        }
        Command::Insert { collection, body } => {
            let tx = client.begin(database, false).await?;
            let insert = match client
                .send_ok(json!({
                    "type": "insert",
                    "tx": tx,
                    "collection": collection,
                    "body": body
                }))
                .await
            {
                Ok(fields) => fields,
                Err(err) => {
                    let _ = client.rollback(tx).await;
                    return Err(err);
                }
            };
            let commit = client.commit(tx).await?;
            let mut output = ok_output(commit);
            if let Some(doc_id) = insert.get("doc_id").cloned() {
                output["id"] = doc_id;
            }
            Ok(output)
        }
        Command::Get { collection, doc_id } => {
            let tx = client.begin(database, true).await?;
            let result = client
                .send_ok(json!({
                    "type": "get",
                    "tx": tx,
                    "collection": collection,
                    "doc_id": doc_id
                }))
                .await;
            let _ = client.rollback(tx).await;
            let fields = result?;
            Ok(json!({
                "query_id": fields.get("query_id").cloned().unwrap_or(Value::Null),
                "document": fields.get("doc").cloned().unwrap_or(Value::Null)
            }))
        }
        Command::Replace {
            collection,
            doc_id,
            body,
        } => {
            server_write_one(
                &mut client,
                database,
                json!({
                    "type": "replace",
                    "collection": collection,
                    "doc_id": doc_id,
                    "body": body
                }),
            )
            .await
        }
        Command::Patch {
            collection,
            doc_id,
            patch,
        } => {
            server_write_one(
                &mut client,
                database,
                json!({
                    "type": "patch",
                    "collection": collection,
                    "doc_id": doc_id,
                    "body": patch
                }),
            )
            .await
        }
        Command::Delete { collection, doc_id } => {
            server_write_one(
                &mut client,
                database,
                json!({
                    "type": "delete",
                    "collection": collection,
                    "doc_id": doc_id
                }),
            )
            .await
        }
        Command::Query {
            collection,
            index,
            range,
            filter,
            order,
            limit,
        } => {
            let tx = client.begin(database, true).await?;
            let mut request = json!({
                "type": "query",
                "tx": tx,
                "collection": collection,
                "index": index,
                "range": range.iter().map(range_expr_json).collect::<Vec<_>>()
            });
            if let Some(object) = request.as_object_mut() {
                if let Some(filter) = filter.as_ref().map(filter_json) {
                    object.insert("filter".to_string(), filter);
                }
                if let Some(order) = order_json(order) {
                    object.insert("order".to_string(), Value::from(order));
                }
                if let Some(limit) = limit {
                    object.insert("limit".to_string(), Value::from(limit));
                }
            }
            let result = client.send_ok(request).await;
            let _ = client.rollback(tx).await;
            let fields = result?;
            Ok(json!({
                "query_id": fields.get("query_id").cloned().unwrap_or(Value::Null),
                "documents": fields.get("docs").cloned().unwrap_or_else(|| Value::Array(Vec::new()))
            }))
        }
        Command::CheckIntegrity => Err(CliError::Usage(
            "check-integrity is currently available only in embedded mode".to_string(),
        )),
    }
}

struct JsonServerClient {
    stream: TcpStream,
    next_msg_id: u32,
}

impl JsonServerClient {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            next_msg_id: 1,
        }
    }

    fn stream_mut(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    async fn begin(
        &mut self,
        database: &str,
        readonly: bool,
    ) -> std::result::Result<u64, CliError> {
        let fields = self
            .send_ok(json!({
                "type": "begin",
                "database": database,
                "readonly": readonly
            }))
            .await?;
        fields
            .get("tx")
            .and_then(Value::as_u64)
            .ok_or_else(|| CliError::WireProtocol("begin response did not include tx".to_string()))
    }

    async fn commit(
        &mut self,
        tx: u64,
    ) -> std::result::Result<serde_json::Map<String, Value>, CliError> {
        self.send_ok(json!({
            "type": "commit",
            "tx": tx
        }))
        .await
    }

    async fn rollback(
        &mut self,
        tx: u64,
    ) -> std::result::Result<serde_json::Map<String, Value>, CliError> {
        self.send_ok(json!({
            "type": "rollback",
            "tx": tx
        }))
        .await
    }

    async fn send_ok(
        &mut self,
        mut message: Value,
    ) -> std::result::Result<serde_json::Map<String, Value>, CliError> {
        let msg_id = self.next_msg_id;
        self.next_msg_id = self.next_msg_id.checked_add(1).ok_or_else(|| {
            CliError::WireProtocol("client message id counter overflowed".to_string())
        })?;

        let Some(object) = message.as_object_mut() else {
            return Err(CliError::WireProtocol(
                "server request must be a JSON object".to_string(),
            ));
        };
        object.insert("id".to_string(), Value::from(msg_id));

        let request = RawFrame::json_text(serde_json::to_vec(&message)?);
        write_frame_with_limit(
            &mut self.stream,
            &request,
            exdb_wire::DEFAULT_MAX_MESSAGE_SIZE,
        )
        .await?;

        loop {
            let response =
                read_frame_with_limit(&mut self.stream, exdb_wire::DEFAULT_MAX_MESSAGE_SIZE)
                    .await?;
            let payload = json_payload(&response)?;
            if payload.get("id").and_then(Value::as_u64) != Some(u64::from(msg_id)) {
                continue;
            }
            return server_ok_fields(payload);
        }
    }
}

async fn server_write_one(
    client: &mut JsonServerClient,
    database: &str,
    mut message: Value,
) -> std::result::Result<Value, CliError> {
    let tx = client.begin(database, false).await?;
    let Some(object) = message.as_object_mut() else {
        return Err(CliError::WireProtocol(
            "server request must be a JSON object".to_string(),
        ));
    };
    object.insert("tx".to_string(), Value::from(tx));

    if let Err(err) = client.send_ok(message).await {
        let _ = client.rollback(tx).await;
        return Err(err);
    }

    let commit = client.commit(tx).await?;
    Ok(ok_output(commit))
}

fn server_ok_fields(
    payload: Value,
) -> std::result::Result<serde_json::Map<String, Value>, CliError> {
    let Value::Object(mut object) = payload else {
        return Err(CliError::WireProtocol(
            "server response payload was not an object".to_string(),
        ));
    };
    let response_type = object
        .remove("type")
        .and_then(|value| value.as_str().map(str::to_string))
        .ok_or_else(|| CliError::WireProtocol("server response missing type".to_string()))?;
    object.remove("id");

    if response_type == "error" {
        let code = object
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("error");
        let message = object
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("server error");
        return Err(CliError::DatabaseOperation(format!("{code}: {message}")));
    }
    if response_type != "ok" {
        return Err(CliError::WireProtocol(format!(
            "expected ok response, got {response_type}"
        )));
    }
    Ok(object)
}

fn ok_output(mut fields: serde_json::Map<String, Value>) -> Value {
    fields.insert("ok".to_string(), Value::Bool(true));
    Value::Object(fields)
}

fn commit_json(result: TransactionResult) -> std::result::Result<Value, CliError> {
    expect_commit_success(result)
}

fn expect_commit_success(result: TransactionResult) -> std::result::Result<Value, CliError> {
    match result {
        TransactionResult::Success { commit_ts, .. } => Ok(json!({
            "ok": true,
            "commit_ts": commit_ts
        })),
        TransactionResult::Conflict { error, .. } => Err(CliError::DatabaseOperation(format!(
            "OCC conflict: {error:?}"
        ))),
        TransactionResult::QuorumLost => Err(CliError::DatabaseOperation(
            "replication quorum lost".to_string(),
        )),
    }
}

fn collection_json(meta: exdb::CollectionMeta) -> Value {
    json!({
        "collection_id": meta.collection_id.0,
        "name": meta.name,
        "primary_root_page": meta.primary_root_page,
        "doc_count": meta.doc_count
    })
}

fn index_json(meta: exdb::IndexMeta) -> Value {
    json!({
        "index_id": meta.index_id.0,
        "collection_id": meta.collection_id.0,
        "name": meta.name,
        "fields": meta.field_paths.iter().map(field_path_json).collect::<Vec<_>>(),
        "state": format!("{:?}", meta.state),
        "root_page": meta.root_page
    })
}

fn field_path_json(path: &FieldPath) -> Value {
    Value::Array(path.segments().iter().cloned().map(Value::String).collect())
}

fn range_expr_json(expr: &RangeExpr) -> Value {
    match expr {
        RangeExpr::Eq(field, value) => {
            json!({ "eq": [field_path_json(field), scalar_json(value)] })
        }
        RangeExpr::Gt(field, value) => {
            json!({ "gt": [field_path_json(field), scalar_json(value)] })
        }
        RangeExpr::Gte(field, value) => {
            json!({ "gte": [field_path_json(field), scalar_json(value)] })
        }
        RangeExpr::Lt(field, value) => {
            json!({ "lt": [field_path_json(field), scalar_json(value)] })
        }
        RangeExpr::Lte(field, value) => {
            json!({ "lte": [field_path_json(field), scalar_json(value)] })
        }
    }
}

fn filter_json(filter: &Filter) -> Value {
    match filter {
        Filter::Eq(field, value) => json!({ "eq": [field_path_json(field), scalar_json(value)] }),
        Filter::Ne(field, value) => json!({ "ne": [field_path_json(field), scalar_json(value)] }),
        Filter::Gt(field, value) => json!({ "gt": [field_path_json(field), scalar_json(value)] }),
        Filter::Gte(field, value) => {
            json!({ "gte": [field_path_json(field), scalar_json(value)] })
        }
        Filter::Lt(field, value) => json!({ "lt": [field_path_json(field), scalar_json(value)] }),
        Filter::Lte(field, value) => {
            json!({ "lte": [field_path_json(field), scalar_json(value)] })
        }
        Filter::In(field, values) => json!({
            "in": [
                field_path_json(field),
                values.iter().map(scalar_json).collect::<Vec<_>>()
            ]
        }),
        Filter::And(filters) => {
            json!({ "and": filters.iter().map(filter_json).collect::<Vec<_>>() })
        }
        Filter::Or(filters) => json!({ "or": filters.iter().map(filter_json).collect::<Vec<_>>() }),
        Filter::Not(filter) => json!({ "not": filter_json(filter) }),
    }
}

fn scalar_json(value: &Scalar) -> Value {
    match value {
        Scalar::Undefined | Scalar::Null => Value::Null,
        Scalar::Int64(value) => Value::from(*value),
        Scalar::Float64(value) => Value::from(*value),
        Scalar::Boolean(value) => Value::from(*value),
        Scalar::String(value) => Value::from(value.clone()),
        Scalar::Bytes(value) => Value::Array(value.iter().copied().map(Value::from).collect()),
        Scalar::Id(value) => Value::from(encode_ulid(value)),
    }
}

fn order_json(order: Option<ScanDirection>) -> Option<&'static str> {
    match order {
        Some(ScanDirection::Forward) => Some("asc"),
        Some(ScanDirection::Backward) => Some("desc"),
        None => None,
    }
}

fn integrity_json(report: exdb_storage_report::IntegrityReport) -> Value {
    json!({
        "ok": report.is_ok(),
        "stats": {
            "page_count": report.stats.page_count,
            "pages_scanned": report.stats.pages_scanned,
            "free_pages": report.stats.free_pages,
            "btree_pages": report.stats.btree_pages,
            "heap_pages": report.stats.heap_pages,
            "overflow_pages": report.stats.overflow_pages,
            "orphan_btree_pages": report.stats.orphan_btree_pages,
            "orphan_heap_pages": report.stats.orphan_heap_pages,
            "double_allocated_pages": report.stats.double_allocated_pages,
            "wal_records_scanned": report.stats.wal_records_scanned,
            "wal_bytes_scanned": report.stats.wal_bytes_scanned,
            "page_type_counts": report.stats.page_type_counts
        },
        "issues": report.issues.into_iter().map(|issue| json!({
            "severity": format!("{:?}", issue.severity),
            "page_id": issue.page_id,
            "message": issue.message
        })).collect::<Vec<_>>()
    })
}

mod exdb_storage_report {
    pub type IntegrityReport = exdb_storage::engine::IntegrityReport;
}

fn write_json(
    out: &mut dyn Write,
    value: &Value,
    pretty: bool,
) -> std::result::Result<(), CliError> {
    if pretty {
        serde_json::to_writer_pretty(&mut *out, value)?;
    } else {
        serde_json::to_writer(&mut *out, value)?;
    }
    writeln!(out)?;
    Ok(())
}

fn json_payload(frame: &RawFrame) -> std::result::Result<Value, CliError> {
    Ok(serde_json::from_slice(&frame.payload)?)
}

fn decode_doc_id(value: &str) -> std::result::Result<exdb::DocId, CliError> {
    decode_ulid(value).map_err(|e| CliError::Usage(format!("invalid document id: {e}")))
}

fn parse_json_arg(value: &str) -> std::result::Result<Value, CliError> {
    serde_json::from_str(value).map_err(CliError::Json)
}

fn parse_field_path_string(value: &str) -> std::result::Result<FieldPath, CliError> {
    let segments = value
        .split('.')
        .map(str::trim)
        .map(str::to_string)
        .collect::<Vec<_>>();
    if segments.iter().any(|segment| segment.is_empty()) {
        return Err(CliError::Usage(format!("invalid field path: {value}")));
    }
    Ok(FieldPath::new(segments))
}

fn parse_field_path(value: Value) -> std::result::Result<FieldPath, CliError> {
    match value {
        Value::String(value) => parse_field_path_string(&value),
        Value::Array(segments) => {
            let mut parsed = Vec::with_capacity(segments.len());
            for segment in segments {
                let Value::String(segment) = segment else {
                    return Err(CliError::Usage(
                        "nested field path segments must be strings".to_string(),
                    ));
                };
                if segment.is_empty() {
                    return Err(CliError::Usage(
                        "field path segment cannot be empty".to_string(),
                    ));
                }
                parsed.push(segment);
            }
            Ok(FieldPath::new(parsed))
        }
        _ => Err(CliError::Usage(
            "field path must be a string or array of strings".to_string(),
        )),
    }
}

fn parse_range_exprs(values: Vec<Value>) -> std::result::Result<Vec<RangeExpr>, CliError> {
    values.into_iter().map(parse_range_expr).collect()
}

fn parse_range_expr(value: Value) -> std::result::Result<RangeExpr, CliError> {
    let (op, args) = parse_single_operator_object(value, "range expression")?;
    let (field, scalar) = parse_field_scalar_args(args, &op)?;
    match op.as_str() {
        "eq" => Ok(RangeExpr::Eq(field, scalar)),
        "gt" => Ok(RangeExpr::Gt(field, scalar)),
        "gte" => Ok(RangeExpr::Gte(field, scalar)),
        "lt" => Ok(RangeExpr::Lt(field, scalar)),
        "lte" => Ok(RangeExpr::Lte(field, scalar)),
        _ => Err(CliError::Usage(format!("unsupported range operator: {op}"))),
    }
}

fn parse_filter(value: Value) -> std::result::Result<Filter, CliError> {
    let (op, args) = parse_single_operator_object(value, "filter")?;
    match op.as_str() {
        "eq" | "ne" | "gt" | "gte" | "lt" | "lte" => {
            let (field, scalar) = parse_field_scalar_args(args, &op)?;
            match op.as_str() {
                "eq" => Ok(Filter::Eq(field, scalar)),
                "ne" => Ok(Filter::Ne(field, scalar)),
                "gt" => Ok(Filter::Gt(field, scalar)),
                "gte" => Ok(Filter::Gte(field, scalar)),
                "lt" => Ok(Filter::Lt(field, scalar)),
                "lte" => Ok(Filter::Lte(field, scalar)),
                _ => unreachable!(),
            }
        }
        "in" => {
            let Value::Array(mut args) = args else {
                return Err(CliError::Usage(
                    "filter operator 'in' expects [field, values]".to_string(),
                ));
            };
            if args.len() != 2 {
                return Err(CliError::Usage(
                    "filter operator 'in' expects [field, values]".to_string(),
                ));
            }
            let values = args.pop().unwrap();
            let field = parse_field_path(args.pop().unwrap())?;
            let Value::Array(values) = values else {
                return Err(CliError::Usage(
                    "filter operator 'in' values must be an array".to_string(),
                ));
            };
            let values = values
                .into_iter()
                .map(parse_scalar)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(Filter::In(field, values))
        }
        "and" | "or" => {
            let Value::Array(values) = args else {
                return Err(CliError::Usage(format!(
                    "filter operator '{op}' expects an array"
                )));
            };
            let filters = values
                .into_iter()
                .map(parse_filter)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if op == "and" {
                Ok(Filter::And(filters))
            } else {
                Ok(Filter::Or(filters))
            }
        }
        "not" => Ok(Filter::Not(Box::new(parse_filter(args)?))),
        _ => Err(CliError::Usage(format!(
            "unsupported filter operator: {op}"
        ))),
    }
}

fn parse_single_operator_object(
    value: Value,
    context: &str,
) -> std::result::Result<(String, Value), CliError> {
    let Value::Object(object) = value else {
        return Err(CliError::Usage(format!("{context} must be an object")));
    };
    if object.len() != 1 {
        return Err(CliError::Usage(format!(
            "{context} must contain exactly one operator"
        )));
    }
    Ok(object.into_iter().next().unwrap())
}

fn parse_field_scalar_args(
    value: Value,
    op: &str,
) -> std::result::Result<(FieldPath, Scalar), CliError> {
    let Value::Array(mut args) = value else {
        return Err(CliError::Usage(format!(
            "operator '{op}' expects [field, value]"
        )));
    };
    if args.len() != 2 {
        return Err(CliError::Usage(format!(
            "operator '{op}' expects [field, value]"
        )));
    }
    let scalar = parse_scalar(args.pop().unwrap())?;
    let field = parse_field_path(args.pop().unwrap())?;
    Ok((field, scalar))
}

fn parse_scalar(value: Value) -> std::result::Result<Scalar, CliError> {
    match value {
        Value::Null => Ok(Scalar::Null),
        Value::Bool(value) => Ok(Scalar::Boolean(value)),
        Value::String(value) => Ok(Scalar::String(value)),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Scalar::Int64(value))
            } else if let Some(value) = value.as_u64() {
                i64::try_from(value)
                    .map(Scalar::Int64)
                    .map_err(|_| CliError::Usage("unsigned scalar exceeds int64".to_string()))
            } else if let Some(value) = value.as_f64() {
                Ok(Scalar::Float64(value))
            } else {
                Err(CliError::Usage("unsupported numeric scalar".to_string()))
            }
        }
        Value::Array(_) | Value::Object(_) => Err(CliError::Usage(
            "array and object values are not valid scalar predicates".to_string(),
        )),
    }
}

fn parse_order(order: Option<&str>) -> std::result::Result<Option<ScanDirection>, CliError> {
    match order {
        None | Some("asc") => Ok(Some(ScanDirection::Forward)),
        Some("desc") => Ok(Some(ScanDirection::Backward)),
        Some(other) => Err(CliError::Usage(format!("unsupported query order: {other}"))),
    }
}

fn next_string(
    args: &mut impl Iterator<Item = String>,
    usage: &'static str,
) -> std::result::Result<String, CliError> {
    args.next()
        .ok_or_else(|| CliError::Usage(format!("expected {usage}")))
}

fn next_path(
    args: &mut impl Iterator<Item = String>,
    flag: &'static str,
) -> std::result::Result<PathBuf, CliError> {
    args.next()
        .map(PathBuf::from)
        .ok_or_else(|| CliError::Usage(format!("{flag} requires a value")))
}

fn next_addr(
    args: &mut impl Iterator<Item = String>,
    flag: &'static str,
) -> std::result::Result<SocketAddr, CliError> {
    let value = next_string(args, flag)?;
    value
        .parse()
        .map_err(|e| CliError::Usage(format!("{flag} expects socket address: {e}")))
}

fn expect_end(args: impl Iterator<Item = String>) -> std::result::Result<(), CliError> {
    let rest = args.collect::<Vec<_>>();
    if rest.is_empty() {
        Ok(())
    } else {
        Err(CliError::Usage(format!(
            "unexpected trailing arguments: {}",
            rest.join(" ")
        )))
    }
}

#[derive(Debug)]
enum CliError {
    Usage(String),
    Database(exdb::DatabaseError),
    DatabaseOperation(String),
    Io(std::io::Error),
    Json(serde_json::Error),
    Wire(exdb_wire::WireError),
    WireProtocol(String),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::Usage(message) => f.write_str(message),
            CliError::Database(err) => write!(f, "{err}"),
            CliError::DatabaseOperation(message) => f.write_str(message),
            CliError::Io(err) => write!(f, "{err}"),
            CliError::Json(err) => write!(f, "{err}"),
            CliError::Wire(err) => write!(f, "{err}"),
            CliError::WireProtocol(message) => f.write_str(message),
        }
    }
}

impl Error for CliError {}

impl From<exdb::DatabaseError> for CliError {
    fn from(value: exdb::DatabaseError) -> Self {
        CliError::Database(value)
    }
}

impl From<std::io::Error> for CliError {
    fn from(value: std::io::Error) -> Self {
        CliError::Io(value)
    }
}

impl From<serde_json::Error> for CliError {
    fn from(value: serde_json::Error) -> Self {
        CliError::Json(value)
    }
}

impl From<exdb_wire::WireError> for CliError {
    fn from(value: exdb_wire::WireError) -> Self {
        CliError::Wire(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    async fn cli(args: Vec<String>) -> Value {
        let mut out = Vec::new();
        run(args, &mut out).await.unwrap();
        serde_json::from_slice(&out).unwrap()
    }

    #[tokio::test]
    async fn embedded_insert_get_and_list_collections() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        let base = vec![
            "--data-path".to_string(),
            db_path.to_string_lossy().to_string(),
        ];

        let mut args = base.clone();
        args.extend(
            ["create-collection", "users"]
                .into_iter()
                .map(str::to_string),
        );
        let created = cli(args).await;
        assert_eq!(created["ok"], true);

        let mut args = base.clone();
        args.extend(
            ["insert", "users", r#"{"name":"Ada","age":37}"#]
                .into_iter()
                .map(str::to_string),
        );
        let inserted = cli(args).await;
        let id = inserted["id"].as_str().unwrap().to_string();
        assert_eq!(inserted["ok"], true);

        let mut args = base.clone();
        args.extend(["get", "users", &id].into_iter().map(str::to_string));
        let fetched = cli(args).await;
        assert_eq!(fetched["document"]["name"], "Ada");
        assert!(fetched["query_id"].is_u64());

        let mut args = base.clone();
        args.push("list-collections".to_string());
        let collections = cli(args).await;
        assert_eq!(collections["collections"][0]["name"], "users");
    }

    #[tokio::test]
    async fn embedded_patch_delete_and_integrity() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        let base = vec![
            "--data-path".to_string(),
            db_path.to_string_lossy().to_string(),
        ];

        let mut args = base.clone();
        args.extend(
            ["create-collection", "users"]
                .into_iter()
                .map(str::to_string),
        );
        cli(args).await;

        let mut args = base.clone();
        args.extend(
            ["insert", "users", r#"{"name":"Grace","role":"dev"}"#]
                .into_iter()
                .map(str::to_string),
        );
        let id = cli(args).await["id"].as_str().unwrap().to_string();

        let mut args = base.clone();
        args.extend(
            ["patch", "users", &id, r#"{"role":"lead"}"#]
                .into_iter()
                .map(str::to_string),
        );
        assert_eq!(cli(args).await["ok"], true);

        let mut args = base.clone();
        args.extend(["get", "users", &id].into_iter().map(str::to_string));
        assert_eq!(cli(args).await["document"]["role"], "lead");

        let mut args = base.clone();
        args.extend(["delete", "users", &id].into_iter().map(str::to_string));
        assert_eq!(cli(args).await["ok"], true);

        let mut args = base.clone();
        args.extend(["get", "users", &id].into_iter().map(str::to_string));
        assert!(cli(args).await["document"].is_null());

        let mut args = base;
        args.push("check-integrity".to_string());
        assert_eq!(cli(args).await["ok"], true);
    }

    #[test]
    fn parse_query_range_filter_and_order() {
        let command = Command::parse(
            [
                "query",
                "users",
                "by_age",
                "--range",
                r#"[{"gte":["age",30]}]"#,
                "--filter",
                r#"{"eq":["active",true]}"#,
                "--order",
                "desc",
                "--limit",
                "10",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        )
        .unwrap();

        let Command::Query {
            range,
            filter,
            order,
            limit,
            ..
        } = command
        else {
            panic!("expected query command");
        };
        assert_eq!(range.len(), 1);
        assert!(filter.is_some());
        assert!(matches!(order, Some(ScanDirection::Backward)));
        assert_eq!(limit, Some(10));
    }

    #[test]
    fn send_json_requires_connect() {
        let err =
            Invocation::parse(["send-json".to_string(), r#"{"id":1}"#.to_string()]).unwrap_err();
        assert!(err.to_string().contains("requires --connect"));
    }

    #[test]
    fn help_does_not_require_command() {
        let invocation = Invocation::parse(["--help".to_string()]).unwrap();
        assert!(invocation.help);
    }

    #[test]
    fn no_command_starts_repl() {
        let invocation = Invocation::parse(Vec::<String>::new()).unwrap();
        assert!(matches!(invocation.command, Command::Repl));
    }

    #[test]
    fn invalid_field_path_is_rejected() {
        let err = Command::parse(
            ["create-index", "users", "bad", "a..b"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("invalid field path"));
    }

    #[test]
    fn pretty_help_output_is_available() {
        assert!(help_text().contains("Embedded commands"));
        assert!(help_text().contains("repl"));
        assert!(help_text().contains("send-json"));
    }

    #[test]
    fn repl_line_parser_preserves_quoted_json() {
        let args =
            parse_repl_line(r#"insert users '{"name":"Ada Lovelace","role":"compiler"}'"#).unwrap();
        assert_eq!(
            args,
            vec![
                "insert",
                "users",
                r#"{"name":"Ada Lovelace","role":"compiler"}"#
            ]
        );
    }

    #[test]
    fn repl_line_parser_rejects_unclosed_quote() {
        let err = parse_repl_line("insert users '{\"name\":\"Ada\"").unwrap_err();
        assert!(err.to_string().contains("unterminated"));
    }

    #[tokio::test]
    async fn embedded_repl_runs_commands_until_exit() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        let mode = Mode::Embedded { data_path: db_path };
        let mut input = std::io::Cursor::new(
            "create-collection users\nlist-collections\nexit\n"
                .as_bytes()
                .to_vec(),
        );
        let mut out = Vec::new();

        run_repl(
            mode,
            DEFAULT_DATABASE.to_string(),
            false,
            &mut input,
            &mut out,
        )
        .await
        .unwrap();

        let output = String::from_utf8(out).unwrap();
        assert!(output.contains(r#""mode":"embedded""#));
        assert!(output.contains(r#""ok":true"#));
        assert!(output.contains(r#""name":"users""#));
    }

    #[tokio::test]
    async fn server_repl_runs_translated_commands_until_quit() {
        let (_dir, addr, shutdown, task) = start_test_server().await;
        let mode = Mode::Server { connect: addr };
        let mut input = std::io::Cursor::new(
            "create-database app\nlist-databases\nquit\n"
                .as_bytes()
                .to_vec(),
        );
        let mut out = Vec::new();

        run_repl(
            mode,
            DEFAULT_DATABASE.to_string(),
            false,
            &mut input,
            &mut out,
        )
        .await
        .unwrap();

        let output = String::from_utf8(out).unwrap();
        assert!(output.contains(r#""mode":"server""#));
        assert!(output.contains(r#""name":"app""#));

        shutdown.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn server_json_ping_round_trip() {
        let (_dir, addr, shutdown, task) = start_test_server().await;

        let output = cli(vec![
            "--connect".to_string(),
            addr.to_string(),
            "send-json".to_string(),
            r#"{"id":1,"type":"ping"}"#.to_string(),
        ])
        .await;
        assert_eq!(output["hello"]["type"], "hello");
        assert_eq!(output["response"]["type"], "pong");

        shutdown.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn server_high_level_commands_round_trip() {
        let (_dir, addr, shutdown, task) = start_test_server().await;
        let connect = addr.to_string();

        let created_db = cli(vec![
            "--connect".to_string(),
            connect.clone(),
            "create-database".to_string(),
            "app".to_string(),
        ])
        .await;
        assert_eq!(created_db["ok"], true);

        let databases = cli(vec![
            "--connect".to_string(),
            connect.clone(),
            "list-databases".to_string(),
        ])
        .await;
        assert_eq!(databases["databases"][0]["name"], "app");
        let usage = &databases["databases"][0]["usage"];
        let disk_usage_bytes = usage["disk_usage_bytes"].as_u64().unwrap();
        let page_store_bytes = usage["page_store_bytes"].as_u64().unwrap();
        let wal_retained_bytes = usage["wal_retained_bytes"].as_u64().unwrap();
        let memory_budget_bytes = usage["memory_budget_bytes"].as_u64().unwrap();
        let buffer_pool_used_frames = usage["buffer_pool_used_frames"].as_u64().unwrap();
        let active_transactions = usage["active_transactions"].as_u64().unwrap();
        let page_count = usage["page_count"].as_u64().unwrap();
        let page_size = usage["page_size"].as_u64().unwrap();

        assert!(disk_usage_bytes > 0);
        assert_eq!(disk_usage_bytes, page_store_bytes + wal_retained_bytes);
        assert_eq!(page_store_bytes, page_count * page_size);
        assert!(memory_budget_bytes >= page_size);
        assert!(buffer_pool_used_frames > 0);
        assert_eq!(active_transactions, 0);

        let mut base = vec![
            "--connect".to_string(),
            connect.clone(),
            "--database".to_string(),
            "app".to_string(),
        ];

        let mut args = base.clone();
        args.extend(
            ["create-collection", "users"]
                .into_iter()
                .map(str::to_string),
        );
        assert_eq!(cli(args).await["ok"], true);

        let mut args = base.clone();
        args.extend(
            ["create-index", "users", "name_idx", "name"]
                .into_iter()
                .map(str::to_string),
        );
        assert_eq!(cli(args).await["ok"], true);

        let mut args = base.clone();
        args.extend(["list-indexes", "users"].into_iter().map(str::to_string));
        let indexes = cli(args).await;
        assert!(
            indexes["indexes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|index| index["name"] == "name_idx")
        );

        let mut args = base.clone();
        args.extend(
            ["insert", "users", r#"{"name":"Ada","role":"dev"}"#]
                .into_iter()
                .map(str::to_string),
        );
        let inserted = cli(args).await;
        let id = inserted["id"].as_str().unwrap().to_string();
        assert_eq!(inserted["ok"], true);

        let mut args = base.clone();
        args.extend(["get", "users", &id].into_iter().map(str::to_string));
        let fetched = cli(args).await;
        assert_eq!(fetched["document"]["name"], "Ada");
        assert!(fetched["query_id"].is_u64());

        let mut args = base.clone();
        args.extend(
            ["query", "users", "_created_at", "--limit", "10"]
                .into_iter()
                .map(str::to_string),
        );
        let queried = cli(args).await;
        assert_eq!(queried["documents"].as_array().unwrap().len(), 1);

        let mut args = base.clone();
        args.extend(
            ["patch", "users", &id, r#"{"role":"lead"}"#]
                .into_iter()
                .map(str::to_string),
        );
        assert_eq!(cli(args).await["ok"], true);

        let mut args = base.clone();
        args.extend(["get", "users", &id].into_iter().map(str::to_string));
        assert_eq!(cli(args).await["document"]["role"], "lead");

        let mut args = base.clone();
        args.extend(["delete", "users", &id].into_iter().map(str::to_string));
        assert_eq!(cli(args).await["ok"], true);

        let mut args = base.clone();
        args.extend(["get", "users", &id].into_iter().map(str::to_string));
        assert!(cli(args).await["document"].is_null());

        base.extend(["drop-collection", "users"].into_iter().map(str::to_string));
        assert_eq!(cli(base).await["ok"], true);

        shutdown.cancel();
        task.await.unwrap().unwrap();
    }

    async fn start_test_server() -> (
        tempfile::TempDir,
        std::net::SocketAddr,
        tokio_util::sync::CancellationToken,
        tokio::task::JoinHandle<std::result::Result<(), exdb_wire::WireError>>,
    ) {
        use exdb::SystemDatabase;
        use exdb_wire::{AuthConfig, ListenConfig, Server, ServerConfig};
        use tokio::net::TcpListener;
        use tokio_util::sync::CancellationToken;

        let dir = tempfile::tempdir().unwrap();
        let registry = std::sync::Arc::new(SystemDatabase::open(dir.path()).await.unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let server = std::sync::Arc::new(Server::new(
            ServerConfig {
                listen: ListenConfig {
                    tcp: Some(addr),
                    tls: None,
                    quic: None,
                    websocket: None,
                    websocket_tls: None,
                },
                tls: None,
                auth: AuthConfig::default(),
                node_role: exdb_wire::DEFAULT_NODE_ROLE.to_string(),
                transaction_promoter: None,
                replica_read_gate: None,
                max_message_size: exdb_wire::DEFAULT_MAX_MESSAGE_SIZE,
                request_queue_capacity: exdb_wire::DEFAULT_REQUEST_QUEUE_CAPACITY,
                response_write_timeout: exdb_wire::DEFAULT_RESPONSE_WRITE_TIMEOUT,
                default_database_config: exdb::DatabaseConfig::default(),
            },
            registry,
        ));
        let shutdown = CancellationToken::new();
        let task = {
            let server = std::sync::Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        wait_for_tcp(addr).await;
        (dir, addr, shutdown, task)
    }

    async fn wait_for_tcp(addr: std::net::SocketAddr) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            match TcpStream::connect(addr).await {
                Ok(_) => return,
                Err(err) if tokio::time::Instant::now() < deadline => {
                    if err.kind() != std::io::ErrorKind::ConnectionRefused {
                        panic!("unexpected connect error while waiting for server: {err}");
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(err) => panic!("server did not start on {addr}: {err}"),
            }
        }
    }
}
