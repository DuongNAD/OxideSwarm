//! Admin-only dashboard HTTP handlers for OXS-AUTH v3 cluster members, pending enrollments,
//! active member connections, and invite tokens (`ENROLL-1d`, design 0051).

use std::collections::HashSet;
use std::net::SocketAddr;

use axum::{
    body::Bytes,
    extract::{rejection::JsonRejection, Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Json, Response},
    Extension,
};
use serde::{Deserialize, Serialize};

use rusty_grid_core::noise_auth::{
    invite_payload_check_hex, MemberEntry, MemberRole, PendingEnrollmentInfo,
};
use rusty_grid_core::GridError;

use crate::auth::AuthIdentity;
use crate::member_connections::ActiveMemberConnection;
use crate::server::{revoke_member_with_teardown, run_member_store_blocking};

use super::{json_error, WebUiState};

const INVITE_PREFIX: &str = "oxs1-invite:";

/// Name of the dashboard caller for the audit log: the token name, or `local` for the loopback
/// bypass.
fn actor(identity: &Option<Extension<AuthIdentity>>) -> &str {
    match identity {
        Some(Extension(id)) if id.is_bypassed => "local",
        Some(Extension(id)) => id.name.as_str(),
        None => "unknown",
    }
}
const MAX_FIELD_LEN: usize = 2048;
const DEFAULT_INVITE_EXPIRES_SECS: u64 = 24 * 3600;

#[derive(Debug, Serialize)]
struct MemberDto {
    id: String,
    name: String,
    public_key: rusty_grid_core::noise_auth::PublicKey,
    fingerprint: String,
    roles: Vec<MemberRole>,
    enrolled_at: Option<u64>,
    last_seen: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    via_invite: Option<String>,
}

impl From<MemberEntry> for MemberDto {
    fn from(entry: MemberEntry) -> Self {
        let fingerprint = entry.public_key.fingerprint();
        Self {
            id: entry.id,
            name: entry.name,
            public_key: entry.public_key,
            fingerprint,
            roles: entry.roles,
            enrolled_at: entry.enrolled_at,
            last_seen: entry.last_seen,
            via_invite: entry.via_invite,
        }
    }
}

#[derive(Debug, Serialize)]
struct MembersListResponse {
    members: Vec<MemberDto>,
    pending: Vec<PendingEnrollmentInfo>,
    active_connections: Vec<ActiveMemberConnection>,
    auth_mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    master_fingerprint: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ApproveMemberPayload {
    pairing_code: String,
    #[serde(default)]
    fingerprint: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RenameMemberPayload {
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateInvitePayload {
    #[serde(default)]
    roles: Option<Vec<MemberRole>>,
    #[serde(default)]
    max_uses: Option<u32>,
    #[serde(default)]
    expires_in_secs: Option<u64>,
    #[serde(default)]
    require_approval: Option<bool>,
    #[serde(default)]
    master_addr: Option<String>,
}

#[derive(Debug, Serialize)]
struct InviteCreatedResponse {
    invite: String,
    invite_id: String,
    roles: Vec<MemberRole>,
    max_uses: u32,
    uses_remaining: u32,
    expires_at: u64,
    require_approval: bool,
    master_fingerprint: String,
}

#[derive(Debug, Serialize)]
struct InvitePayloadJson<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    m: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    t: Option<&'a str>,
    mk: String,
    id: &'a str,
    s: String,
    exp: u64,
    r: &'a [MemberRole],
    ap: bool,
}

fn parse_json<T>(payload: Result<Json<T>, JsonRejection>) -> Result<T, (StatusCode, String)> {
    match payload {
        Ok(Json(value)) => Ok(value),
        Err(rejection) => {
            let status = match rejection.status() {
                StatusCode::UNPROCESSABLE_ENTITY | StatusCode::UNSUPPORTED_MEDIA_TYPE => {
                    StatusCode::BAD_REQUEST
                }
                other => other,
            };
            Err((status, rejection.body_text()))
        }
    }
}

fn map_member_store_error(err: GridError, not_found_prefix: &str) -> Response {
    match err {
        GridError::Config(inner) if inner.starts_with(not_found_prefix) => {
            json_error(StatusCode::NOT_FOUND, inner)
        }
        GridError::Config(inner) => json_error(StatusCode::BAD_REQUEST, inner),
        other => json_error(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    }
}

fn normalize_pairing_code(raw: &str) -> Result<String, String> {
    let clean: String = raw
        .trim()
        .chars()
        .filter(|c| *c != '-' && !c.is_whitespace())
        .collect();
    if clean.len() != 6 || !clean.bytes().all(|b| b.is_ascii_digit()) {
        return Err("pairing_code must be exactly 6 digits".into());
    }
    Ok(clean)
}

fn normalize_fingerprint(raw: &str) -> Result<String, String> {
    let clean: String = raw.trim().replace('-', "").to_ascii_lowercase();
    if clean.len() != 16 || !clean.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("fingerprint must be 16 hexadecimal characters".into());
    }
    Ok(clean)
}

fn field_ok(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_FIELD_LEN && !value.chars().any(char::is_whitespace)
}

fn resolve_invite_master_addr(
    server_addr: SocketAddr,
    override_addr: Option<&str>,
) -> Result<Option<String>, String> {
    if let Some(raw) = override_addr {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            if !field_ok(trimmed) {
                return Err(
                    "master_addr must not contain whitespace or exceed 2048 characters".into(),
                );
            }
            return Ok(Some(trimmed.to_string()));
        }
    }
    let ip = if server_addr.ip().is_unspecified() {
        rusty_grid_core::discovery::get_local_ip_or_loopback()
    } else {
        server_addr.ip()
    };
    Ok(Some(SocketAddr::new(ip, server_addr.port()).to_string()))
}

/// Encodes `bytes` using RFC 4648 URL-safe base64 without padding (`=` omitted), matching
/// `base64::engine::general_purpose::URL_SAFE_NO_PAD`.
fn base64url_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((triple >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((triple >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((triple >> 6) & 0x3f) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(TABLE[(triple & 0x3f) as usize] as char);
        }
    }
    out
}

/// `GET /api/members`: returns enrolled members (with public key fingerprints, never private keys),
/// pending enrollment requests (never `pairing_code`), and active member connections (never
/// `CancellationToken`).
pub(super) async fn api_get_members(State(state): State<WebUiState>) -> Response {
    let members: Vec<MemberDto> = state
        .auth_ctx
        .members
        .list_members()
        .into_iter()
        .map(MemberDto::from)
        .collect();
    let pending = state.auth_ctx.members.list_pending();
    let active_connections = state.auth_ctx.member_connections.list_active();
    let master_fingerprint = state
        .auth_ctx
        .master_noise_key
        .as_ref()
        .map(|kp| kp.fingerprint());

    Json(MembersListResponse {
        members,
        pending,
        active_connections,
        auth_mode: state.auth_ctx.auth_mode.to_string(),
        master_fingerprint,
    })
    .into_response()
}

/// `POST /api/members/{target}/approve`: verifies the 6-digit pairing code (and optional master key
/// fingerprint) and enrolls the pending machine.
pub(super) async fn api_approve_member(
    State(state): State<WebUiState>,
    Path(target): Path<String>,
    identity: Option<Extension<AuthIdentity>>,
    payload: Result<Json<ApproveMemberPayload>, JsonRejection>,
) -> Response {
    let payload = match parse_json(payload) {
        Ok(p) => p,
        Err((status, msg)) => return json_error(status, msg),
    };
    let clean_code = match normalize_pairing_code(&payload.pairing_code) {
        Ok(code) => code,
        Err(msg) => return json_error(StatusCode::BAD_REQUEST, msg),
    };
    let formatted_code = match payload
        .fingerprint
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(fp) => match normalize_fingerprint(fp) {
            Ok(clean_fp) => format!("{clean_code}@{clean_fp}"),
            Err(msg) => return json_error(StatusCode::BAD_REQUEST, msg),
        },
        None => clean_code,
    };

    let members_store = state.auth_ctx.members.clone();
    match run_member_store_blocking(move || members_store.approve_pending(&target, &formatted_code))
        .await
    {
        Ok(entry) => {
            tracing::info!(actor = actor(&identity), member = %entry.id, name = %entry.name, "dashboard: member approved");
            (StatusCode::OK, Json(MemberDto::from(entry))).into_response()
        }
        Err(err) => map_member_store_error(err, "no pending"),
    }
}

/// `POST /api/members/{target}/reject`: rejects a pending enrollment without consuming invite uses.
pub(super) async fn api_reject_member(
    State(state): State<WebUiState>,
    Path(target): Path<String>,
    identity: Option<Extension<AuthIdentity>>,
) -> Response {
    let members_store = state.auth_ctx.members.clone();
    match run_member_store_blocking(move || members_store.reject_pending(&target)).await {
        Ok(resolved_id) => {
            tracing::info!(actor = actor(&identity), pending = %resolved_id, "dashboard: enrollment rejected");
            (
                StatusCode::OK,
                Json(serde_json::json!({ "rejected": resolved_id })),
            )
                .into_response()
        }
        Err(err) => map_member_store_error(err, "no pending"),
    }
}

/// `POST /api/members/{target}/revoke`: revokes an enrolled member, closes all of its live
/// connections across TCP/P2P lanes, and requeues any tasks on its workers immediately.
pub(super) async fn api_revoke_member(
    State(state): State<WebUiState>,
    Path(target): Path<String>,
    identity: Option<Extension<AuthIdentity>>,
) -> Response {
    match revoke_member_with_teardown(
        target,
        None,
        &state.auth_ctx,
        &state.registry,
        &state.queue,
        &state.scheduler_notify,
        &state.waiters,
        Some(&state.broadcast_tx),
    )
    .await
    {
        Ok(outcome) => {
            tracing::info!(actor = actor(&identity), member = %outcome.member_id, "dashboard: member revoked");
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "member_id": outcome.member_id,
                    "name": outcome.name,
                    "disconnected_sessions": outcome.disconnected_sessions,
                    "revoked_workers": outcome.revoked_workers,
                })),
            )
                .into_response()
        }
        Err(err) => map_member_store_error(err, "no member"),
    }
}

/// `POST /api/members/{target}/rename`: renames an enrolled member by ID, prefix, or current name.
pub(super) async fn api_rename_member(
    State(state): State<WebUiState>,
    Path(target): Path<String>,
    identity: Option<Extension<AuthIdentity>>,
    payload: Result<Json<RenameMemberPayload>, JsonRejection>,
) -> Response {
    let payload = match parse_json(payload) {
        Ok(p) => p,
        Err((status, msg)) => return json_error(status, msg),
    };
    let members_store = state.auth_ctx.members.clone();
    match run_member_store_blocking(move || members_store.rename_member(&target, &payload.name))
        .await
    {
        Ok(entry) => {
            tracing::info!(actor = actor(&identity), member = %entry.id, name = %entry.name, "dashboard: member renamed");
            (StatusCode::OK, Json(MemberDto::from(entry))).into_response()
        }
        Err(err) => map_member_store_error(err, "no member"),
    }
}

/// `GET /api/invites`: lists cluster invites (`InviteSummary`; never exposes `invite_secret` or the
/// encoded `oxs1-invite:...` string).
pub(super) async fn api_list_invites(State(state): State<WebUiState>) -> Response {
    let invites = state.auth_ctx.members.list_invites();
    Json(serde_json::json!({ "invites": invites })).into_response()
}

/// `POST /api/invites`: creates an invite token (returned once in the response as `invite`),
/// enforcing `--auth members|both`, persisted `master_noise.key`, and Admin single-use/approval
/// constraints.
pub(super) async fn api_create_invite(
    State(state): State<WebUiState>,
    identity: Option<Extension<AuthIdentity>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let payload = if body.iter().all(|b| b.is_ascii_whitespace()) {
        CreateInvitePayload {
            roles: None,
            max_uses: None,
            expires_in_secs: None,
            require_approval: None,
            master_addr: None,
        }
    } else {
        let is_json = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| {
                ct.split(';')
                    .next()
                    .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"))
            });
        if !is_json {
            return json_error(
                StatusCode::BAD_REQUEST,
                "Expected request with `Content-Type: application/json`",
            );
        }
        match serde_json::from_slice::<CreateInvitePayload>(&body) {
            Ok(p) => p,
            Err(err) => return json_error(StatusCode::BAD_REQUEST, err.to_string()),
        }
    };

    if !state.auth_ctx.auth_mode.allows_members() {
        return json_error(
            StatusCode::BAD_REQUEST,
            "cannot create an invite when the master is configured with --auth secret",
        );
    }

    let raw_roles = payload.roles.unwrap_or_else(|| vec![MemberRole::Worker]);
    let mut seen = HashSet::new();
    let mut roles = Vec::with_capacity(raw_roles.len());
    for role in raw_roles {
        if seen.insert(role) {
            roles.push(role);
        }
    }
    if roles.is_empty() {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invite must grant at least one role",
        );
    }

    let max_uses = payload.max_uses.unwrap_or(1);
    let expires_in_secs = payload
        .expires_in_secs
        .unwrap_or(DEFAULT_INVITE_EXPIRES_SECS);
    let require_approval = payload.require_approval.unwrap_or(true);

    let master_addr =
        match resolve_invite_master_addr(state.server_addr, payload.master_addr.as_deref()) {
            Ok(addr) => addr,
            Err(msg) => return json_error(StatusCode::BAD_REQUEST, msg),
        };
    let p2p_ticket = state
        .auth_ctx
        .p2p_ticket
        .clone()
        .or_else(|| state.p2p_ticket.clone());
    if p2p_ticket.as_deref().is_some_and(|t| !field_ok(t)) {
        return json_error(StatusCode::BAD_REQUEST, "invalid master p2p_ticket");
    }

    let auth_ctx = state.auth_ctx.clone();
    let create_res = run_member_store_blocking(move || {
        let pk = auth_ctx.ensure_v3_key_persisted()?;
        let rec =
            auth_ctx
                .members
                .create_invite(roles, max_uses, expires_in_secs, require_approval)?;
        Ok((pk, rec))
    })
    .await;

    let (master_key, rec) = match create_res {
        Ok(res) => res,
        Err(err) => return map_member_store_error(err, "no invite"),
    };

    let payload_json = InvitePayloadJson {
        m: master_addr.as_deref(),
        t: p2p_ticket.as_deref(),
        mk: master_key.to_hex(),
        id: &rec.invite_id,
        s: rec.invite_secret.to_hex(),
        exp: rec.expires_at,
        r: &rec.roles,
        ap: rec.require_approval,
    };
    let json_bytes = match serde_json::to_vec(&payload_json) {
        Ok(bytes) => bytes,
        Err(err) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };
    tracing::info!(
        actor = actor(&identity),
        invite = %rec.invite_id,
        roles = ?rec.roles,
        max_uses = rec.max_uses,
        require_approval = rec.require_approval,
        "dashboard: invite created"
    );
    let encoded_payload = base64url_encode(&json_bytes);
    let check = invite_payload_check_hex(&encoded_payload);
    let invite = format!("{INVITE_PREFIX}{encoded_payload}.{check}");

    (
        StatusCode::OK,
        Json(InviteCreatedResponse {
            invite,
            invite_id: rec.invite_id,
            roles: rec.roles,
            max_uses: rec.max_uses,
            uses_remaining: rec.max_uses,
            expires_at: rec.expires_at,
            require_approval: rec.require_approval,
            master_fingerprint: master_key.fingerprint(),
        }),
    )
        .into_response()
}

/// `DELETE /api/invites/{id}`: revokes an invite by exact ID or unique `>= 4`-hex prefix.
pub(super) async fn api_revoke_invite(
    State(state): State<WebUiState>,
    Path(id): Path<String>,
    identity: Option<Extension<AuthIdentity>>,
) -> Response {
    let members_store = state.auth_ctx.members.clone();
    match run_member_store_blocking(move || members_store.revoke_invite(&id)).await {
        Ok(resolved_id) => {
            tracing::info!(actor = actor(&identity), invite = %resolved_id, "dashboard: invite revoked");
            (
                StatusCode::OK,
                Json(serde_json::json!({ "revoked": resolved_id })),
            )
                .into_response()
        }
        Err(err) => map_member_store_error(err, "no invite"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64url_encode_matches_rfc4648_no_pad() {
        assert_eq!(base64url_encode(b""), "");
        assert_eq!(base64url_encode(b"f"), "Zg");
        assert_eq!(base64url_encode(b"fo"), "Zm8");
        assert_eq!(base64url_encode(b"foo"), "Zm9v");
        assert_eq!(base64url_encode(b"foob"), "Zm9vYg");
        assert_eq!(base64url_encode(b"fooba"), "Zm9vYmE");
        assert_eq!(base64url_encode(b"foobar"), "Zm9vYmFy");
        // Verify URL-safe alphabet (+ -> -, / -> _).
        assert_eq!(base64url_encode(&[0xfb, 0xff, 0xbf]), "-_-_");
    }

    #[test]
    fn normalize_pairing_code_and_fingerprint_validate_strictly() {
        assert_eq!(normalize_pairing_code("418 302").unwrap(), "418302");
        assert_eq!(normalize_pairing_code("418-302").unwrap(), "418302");
        assert!(normalize_pairing_code("12345").is_err());
        assert!(normalize_pairing_code("12345a").is_err());

        assert_eq!(
            normalize_fingerprint("ABCD-1234-ef56-7890").unwrap(),
            "abcd1234ef567890"
        );
        assert!(normalize_fingerprint("short").is_err());
        assert!(normalize_fingerprint("zzzz123456789012").is_err());
    }
}
