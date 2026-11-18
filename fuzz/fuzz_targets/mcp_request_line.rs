#![no_main]
//! LV-18 target 7: the JSON-RPC reader of `oxideswarm mcp`, which reads what a coding agent writes
//! to the server's standard input, one request per line.
//!
//! Input: the bytes the server reads. They are cut into lines as `run_stdio_server` reads them
//! (at `\n`, one `\r` before it removed), and reading stops at the first line that is not UTF-8,
//! where the server stops too. Each line goes to `parse_request_line`; a `tools/call` request then
//! goes to `parse_tool_call`, the checks the server applies to its `params` before it dispatches
//! (`validate_tool_arguments` included). No master and no network are involved.
//!
//! Checks, besides no panic:
//! - an accepted request has `jsonrpc` `"2.0"`, a method, an `id` that is a string, a number or
//!   null, and `params` that are absent, null, an object or an array; written again as one line,
//!   it reads back to the same request. Requests with a float in `id` or `params` are left out of
//!   that comparison: `serde_json` is built without `float_roundtrip`, so a float written and read
//!   again need not keep its bits, or may then be out of range;
//! - an error response serializes (the server would send nothing otherwise) and carries no
//!   result, a message and the JSON-RPC code of the step that refused it; a line that is not JSON
//!   gets no id, and a refused tool call gets the request's id;
//! - an accepted tool call names a tool of `tools/list`, with arguments that are an object and that
//!   `validate_tool_arguments` accepts again.

use std::sync::OnceLock;

use libfuzzer_sys::fuzz_target;
use rusty_grid_cli::mcp::{
    available_tools, parse_request_line, parse_tool_call, validate_tool_arguments, JsonRpcRequest,
    JsonRpcResponse, INVALID_PARAMS, INVALID_REQUEST, PARSE_ERROR,
};
use serde_json::{Map, Value};

fn tool_names() -> &'static [&'static str] {
    static NAMES: OnceLock<Vec<&'static str>> = OnceLock::new();
    NAMES.get_or_init(|| available_tools().into_iter().map(|t| t.name).collect())
}

fn has_float(value: &Value) -> bool {
    match value {
        Value::Number(n) => n.is_f64(),
        Value::Array(items) => items.iter().any(has_float),
        Value::Object(map) => map.values().any(has_float),
        _ => false,
    }
}

/// The request written again as one line, with `id` and `params` only when it has them.
fn request_line(request: &JsonRpcRequest) -> String {
    let mut map = Map::new();
    map.insert("jsonrpc".into(), Value::String(request.jsonrpc.clone()));
    if let Some(id) = &request.id {
        map.insert("id".into(), id.clone());
    }
    map.insert("method".into(), Value::String(request.method.clone()));
    if let Some(params) = &request.params {
        map.insert("params".into(), params.clone());
    }
    serde_json::to_string(&Value::Object(map)).expect("a request serializes")
}

/// Checks an error response and returns its code.
fn check_error(response: &JsonRpcResponse, codes: &[i64]) -> i64 {
    serde_json::to_string(response).expect("an error response serializes");
    assert_eq!(response.jsonrpc, "2.0");
    assert!(response.result.is_none(), "an error response has no result");
    let error = response
        .error
        .as_ref()
        .expect("an error response has an error");
    assert!(
        codes.contains(&error.code),
        "unexpected code {}",
        error.code
    );
    assert!(!error.message.is_empty(), "an error has a message");
    error.code
}

fn check_tool_call(request: &JsonRpcRequest) {
    let params = request.params.clone().unwrap_or(Value::Null);
    match parse_tool_call(&request.id, &params) {
        Ok((name, arguments)) => {
            assert!(
                tool_names().contains(&name.as_str()),
                "an accepted tool call names a tool of tools/list: {name}"
            );
            assert!(arguments.is_object(), "tool arguments are an object");
            assert_eq!(validate_tool_arguments(&name, &arguments), Ok(()));
        }
        Err(response) => {
            check_error(&response, &[INVALID_PARAMS]);
            assert_eq!(
                response.id, request.id,
                "the reply carries the request's id"
            );
        }
    }
}

fn check_line(line: &str) {
    let request = match parse_request_line(line) {
        Ok(Some(request)) => request,
        Ok(None) => return,
        Err(response) => {
            if check_error(&response, &[PARSE_ERROR, INVALID_REQUEST]) == PARSE_ERROR {
                assert!(response.id.is_none(), "a line that is not JSON has no id");
            }
            return;
        }
    };

    assert_eq!(request.jsonrpc, "2.0");
    assert!(!request.method.is_empty(), "a request has a method");
    if let Some(id) = &request.id {
        assert!(id.is_string() || id.is_number() || id.is_null(), "id {id}");
    }
    if let Some(params) = &request.params {
        assert!(
            params.is_null() || params.is_object() || params.is_array(),
            "params {params}"
        );
    }

    let floats = request.id.as_ref().is_some_and(has_float)
        || request.params.as_ref().is_some_and(has_float);
    if !floats {
        let text = request_line(&request);
        let again = parse_request_line(&text)
            .unwrap_or_else(|e| panic!("{text} is refused again: {e:?}"))
            .unwrap_or_else(|| panic!("{text} is dropped again"));
        assert_eq!(again.jsonrpc, request.jsonrpc);
        assert_eq!(again.id, request.id);
        assert_eq!(again.method, request.method);
        assert_eq!(again.params, request.params);
    }

    if request.id.is_some() && request.method == "tools/call" {
        check_tool_call(&request);
    }
}

fuzz_target!(|data: &[u8]| {
    let mut rest = data;
    while !rest.is_empty() {
        let (raw, next) = match rest.iter().position(|&b| b == b'\n') {
            Some(i) => {
                let line = &rest[..i];
                (line.strip_suffix(b"\r").unwrap_or(line), &rest[i + 1..])
            }
            None => (rest, &rest[rest.len()..]),
        };
        let Ok(line) = std::str::from_utf8(raw) else {
            return;
        };
        check_line(line);
        rest = next;
    }
});
