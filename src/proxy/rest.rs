//! REST/JSON pass-through for Pub/Sub's HTTP API.
//!
//! Not every Pub/Sub client speaks gRPC. The service also exposes a REST/JSON API
//! (`POST /v1/projects/p/topics/t:publish` and friends), which the emulator serves
//! on the same port as gRPC, and which several client libraries — and anything
//! reaching for `curl` — use instead. Those clients speak HTTP/1.1 to a port the
//! gRPC proxy answers only in HTTP/2, so without this module they cannot connect
//! through the monitor at all.
//!
//! This is the same bargain the gRPC side strikes: relay the request to the
//! upstream verbatim and hand the response back untouched, observing publishes,
//! pulls and acks in passing. Bodies are buffered rather than streamed — the REST
//! surface is unary JSON, so there is no streaming call to spoil — and anything
//! this module fails to make sense of is still forwarded faithfully; it just goes
//! uncounted.

use std::borrow::Cow;
use std::io::Read as _;
use std::net::SocketAddr;
use std::sync::Arc;

use base64::Engine as _;
use base64::alphabet;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use bytes::Bytes;
use http::{HeaderMap, HeaderName, Method, Request, Response, StatusCode, Uri, header};
use http_body_util::{BodyExt as _, Full, Limited};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde_json::Value;
use tonic::transport::server::TcpConnectInfo;

use crate::observe::{Observation, ObservationSink, PublishedMessage};
use crate::proxy::MAX_MESSAGE_SIZE;

/// Headers that are meaningful only for a single hop and must not be relayed to
/// the next one (RFC 9110 §7.6.1). Everything else — including `authorization`
/// and Google's `x-goog-*` routing headers — is passed through untouched.
const HOP_BY_HOP: [HeaderName; 8] = [
    header::CONNECTION,
    header::PROXY_AUTHENTICATE,
    header::PROXY_AUTHORIZATION,
    header::TE,
    header::TRAILER,
    header::TRANSFER_ENCODING,
    header::UPGRADE,
    HeaderName::from_static("keep-alive"),
];

/// Base64 decoder for the `data` field of a REST message.
///
/// Proto3's JSON mapping says a `bytes` field is base64 and that decoders must
/// accept both the standard and the URL-safe alphabet, with or without padding —
/// so we try the standard alphabet first and fall back to the URL-safe one, both
/// indifferent to padding.
const BASE64_STANDARD: GeneralPurpose = GeneralPurpose::new(
    &alphabet::STANDARD,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);
const BASE64_URL_SAFE: GeneralPurpose = GeneralPurpose::new(
    &alphabet::URL_SAFE,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

/// Relays Pub/Sub REST calls to the upstream server while observing the traffic.
pub(crate) struct RestProxy {
    client: Client<HttpConnector, Full<Bytes>>,
    /// The upstream `host:port`, as given to the monitor.
    upstream: String,
    sink: ObservationSink,
    /// Per-message payload bytes captured for the recent-messages view; larger
    /// payloads are truncated to this many bytes.
    payload_cap: usize,
}

impl RestProxy {
    pub(crate) fn new(upstream: String, sink: ObservationSink, payload_cap: usize) -> Self {
        Self {
            client: Client::builder(TokioExecutor::new()).build_http(),
            upstream,
            sink,
            payload_cap,
        }
    }

    /// Relay one REST call upstream and return its response, observing the
    /// traffic in passing.
    pub(crate) async fn handle(
        self: Arc<Self>,
        request: Request<axum::body::Body>,
    ) -> Response<axum::body::Body> {
        let peer = request
            .extensions()
            .get::<TcpConnectInfo>()
            .and_then(TcpConnectInfo::remote_addr);
        let (parts, body) = request.into_parts();

        let body = match Limited::new(body, MAX_MESSAGE_SIZE).collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(err) => {
                tracing::warn!(%err, path = parts.uri.path(), "REST request body rejected");
                return error_response(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "request body exceeded the proxy's size limit",
                );
            }
        };

        let path_and_query = parts
            .uri
            .path_and_query()
            .map_or_else(|| parts.uri.path().to_owned(), ToString::to_string);
        let uri = match format!("http://{}{path_and_query}", self.upstream).parse::<Uri>() {
            Ok(uri) => uri,
            Err(err) => {
                tracing::warn!(%err, %path_and_query, "could not build upstream REST URI");
                return error_response(StatusCode::BAD_GATEWAY, "malformed request URI");
            }
        };

        let mut upstream_request = Request::new(Full::new(body.clone()));
        *upstream_request.method_mut() = parts.method.clone();
        *upstream_request.uri_mut() = uri;
        // The upstream connection is ours, so the client's `host` (naming this
        // proxy) is dropped and hyper fills in the real one.
        *upstream_request.headers_mut() = relayed_headers(&parts.headers, true);

        let response = match self.client.request(upstream_request).await {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!(
                    %err,
                    upstream = %self.upstream,
                    path = parts.uri.path(),
                    "upstream REST request failed",
                );
                return error_response(StatusCode::BAD_GATEWAY, "upstream request failed");
            }
        };

        let (response_parts, response_body) = response.into_parts();
        let response_body = match Limited::new(response_body, MAX_MESSAGE_SIZE)
            .collect()
            .await
        {
            Ok(collected) => collected.to_bytes(),
            Err(err) => {
                tracing::warn!(%err, path = parts.uri.path(), "upstream REST response body failed");
                return error_response(StatusCode::BAD_GATEWAY, "upstream response failed");
            }
        };

        self.observe(
            &parts.method,
            parts.uri.path(),
            &parts.headers,
            &body,
            response_parts.status,
            &response_parts.headers,
            &response_body,
            peer,
        );

        // Rebuild the response around the buffered body: the length is now known,
        // so any chunked framing the upstream used is replaced by `content-length`.
        let mut relayed = Response::new(axum::body::Body::from(response_body.clone()));
        *relayed.status_mut() = response_parts.status;
        *relayed.version_mut() = parts.version;
        *relayed.headers_mut() = relayed_headers(&response_parts.headers, false);
        relayed.headers_mut().remove(header::CONTENT_LENGTH);
        if let Ok(length) = response_body.len().to_string().parse() {
            relayed.headers_mut().insert(header::CONTENT_LENGTH, length);
        }
        relayed
    }

    /// Fold a completed REST call into the observed state, if it is one of the
    /// calls that carries traffic we count. Anything else is ignored.
    #[expect(
        clippy::too_many_arguments,
        reason = "the observable facts of one relayed call; grouping them in a struct \
                  would only move the same list one level out"
    )]
    fn observe(
        &self,
        method: &Method,
        path: &str,
        request_headers: &HeaderMap,
        request_body: &[u8],
        status: StatusCode,
        response_headers: &HeaderMap,
        response_body: &[u8],
        peer: Option<SocketAddr>,
    ) {
        // A rejected call moved no messages, so it must not be counted.
        if method != Method::POST || !status.is_success() {
            return;
        }
        let Some((resource, verb)) = resource_and_verb(path) else {
            return;
        };

        match verb {
            "publish" if resource.contains("/topics/") => {
                let Some(body) = decoded(request_headers, request_body) else {
                    return;
                };
                let messages = self.captured_messages(&body);
                if messages.is_empty() {
                    return;
                }
                tracing::debug!(
                    rpc = "publish",
                    transport = "rest",
                    topic = %resource,
                    messages = messages.len(),
                    "observed REST Publish",
                );
                self.sink.observe(Observation::Publish {
                    topic: resource.to_owned(),
                    peer,
                    messages,
                });
            }
            "pull" if resource.contains("/subscriptions/") => {
                let Some(body) = decoded(response_headers, response_body) else {
                    return;
                };
                let messages = json_array_len(&body, "receivedMessages");
                if messages > 0 {
                    self.sink.observe(Observation::Deliver {
                        subscription: resource.to_owned(),
                        peer,
                        messages,
                    });
                }
            }
            "acknowledge" if resource.contains("/subscriptions/") => {
                let Some(body) = decoded(request_headers, request_body) else {
                    return;
                };
                let messages = json_array_len(&body, "ackIds");
                if messages > 0 {
                    self.sink.observe(Observation::Ack {
                        subscription: resource.to_owned(),
                        peer,
                        messages,
                    });
                }
            }
            _ => {}
        }
    }

    /// Snapshot the messages of a REST `publish` body for the recent-messages
    /// view. A body we cannot parse yields no messages (and no count).
    fn captured_messages(&self, body: &[u8]) -> Vec<PublishedMessage> {
        let Ok(json) = serde_json::from_slice::<Value>(body) else {
            tracing::debug!("REST publish body was not JSON; not counted");
            return Vec::new();
        };
        json.get("messages")
            .and_then(Value::as_array)
            .map(|messages| messages.iter().map(|m| self.capture(m)).collect())
            .unwrap_or_default()
    }

    /// Capture one REST message: its base64 `data` (capped) and its attributes.
    fn capture(&self, message: &Value) -> PublishedMessage {
        let mut data = message
            .get("data")
            .and_then(Value::as_str)
            .map(decode_base64)
            .unwrap_or_default();
        let attributes = message
            .get("attributes")
            .and_then(Value::as_object)
            .map(|attributes| {
                attributes
                    .iter()
                    .filter_map(|(key, value)| {
                        value.as_str().map(|value| (key.clone(), value.to_owned()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let original_len = data.len();
        let truncated = original_len > self.payload_cap;
        data.truncate(self.payload_cap);
        PublishedMessage {
            data,
            attributes,
            original_len,
            truncated,
        }
    }
}

/// Split `/v1/projects/p/topics/t:publish` into the resource name
/// (`projects/p/topics/t`, the same form the gRPC field carries) and the verb
/// (`publish`). Returns `None` for any other shape of path.
fn resource_and_verb(path: &str) -> Option<(&str, &str)> {
    let (resource, verb) = path.strip_prefix("/v1/")?.rsplit_once(':')?;
    (!resource.is_empty() && !verb.is_empty()).then_some((resource, verb))
}

/// Copy the headers worth relaying to the next hop, dropping the hop-by-hop ones
/// (and, on the way upstream, `host`, which named this proxy).
fn relayed_headers(headers: &HeaderMap, drop_host: bool) -> HeaderMap {
    let mut relayed = headers.clone();
    for name in HOP_BY_HOP {
        relayed.remove(name);
    }
    if drop_host {
        relayed.remove(header::HOST);
    }
    relayed
}

/// The body as the peer meant it, undoing any `content-encoding` so it can be
/// parsed. `None` for an encoding we cannot undo — the body is still relayed
/// untouched, it just goes unobserved.
fn decoded<'a>(headers: &HeaderMap, body: &'a [u8]) -> Option<Cow<'a, [u8]>> {
    match headers
        .get(header::CONTENT_ENCODING)
        .map(|value| value.as_bytes())
    {
        None => Some(Cow::Borrowed(body)),
        Some(b"identity") => Some(Cow::Borrowed(body)),
        Some(b"gzip" | b"x-gzip") => {
            let mut decoded = Vec::new();
            match flate2::read::GzDecoder::new(body).read_to_end(&mut decoded) {
                Ok(_) => Some(Cow::Owned(decoded)),
                Err(err) => {
                    tracing::debug!(%err, "could not gunzip a REST body; not counted");
                    None
                }
            }
        }
        Some(encoding) => {
            tracing::debug!(
                encoding = %String::from_utf8_lossy(encoding),
                "unhandled REST content-encoding; not counted",
            );
            None
        }
    }
}

/// The length of a top-level JSON array field, or `0` if the body is not JSON or
/// carries no such array.
fn json_array_len(body: &[u8], field: &str) -> u64 {
    serde_json::from_slice::<Value>(body)
        .ok()
        .as_ref()
        .and_then(|json| json.get(field))
        .and_then(Value::as_array)
        .map_or(0, |values| values.len() as u64)
}

/// Decode a proto3-JSON `bytes` value, accepting either base64 alphabet. An
/// undecodable value yields empty data rather than dropping the whole message.
fn decode_base64(value: &str) -> Vec<u8> {
    BASE64_STANDARD
        .decode(value)
        .or_else(|_| BASE64_URL_SAFE.decode(value))
        .unwrap_or_else(|err| {
            tracing::debug!(%err, "undecodable base64 in a REST message payload");
            Vec::new()
        })
}

/// A JSON error in the shape Pub/Sub's REST API uses, so a client-under-test
/// parses a proxy failure the same way it would a server one.
fn error_response(status: StatusCode, message: &str) -> Response<axum::body::Body> {
    let body = serde_json::json!({
        "error": { "code": status.as_u16(), "message": message, "status": status.canonical_reason() }
    })
    .to_string();
    let mut response = Response::new(axum::body::Body::from(body));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_a_rest_path_into_resource_and_verb() {
        assert_eq!(
            resource_and_verb("/v1/projects/p/topics/t:publish"),
            Some(("projects/p/topics/t", "publish"))
        );
        assert_eq!(
            resource_and_verb("/v1/projects/p/subscriptions/s:acknowledge"),
            Some(("projects/p/subscriptions/s", "acknowledge"))
        );
        // Verb-less resource paths (topic create/get/delete) carry no traffic.
        assert_eq!(resource_and_verb("/v1/projects/p/topics/t"), None);
        assert_eq!(resource_and_verb("/健康"), None);
    }

    #[test]
    fn decodes_both_base64_alphabets() {
        // `~~~?` — bytes whose standard encoding ends `+8/` and whose URL-safe
        // encoding ends `-8_`, so each alphabet is genuinely exercised.
        assert_eq!(decode_base64("fn5+Pw=="), b"~~~?");
        assert_eq!(decode_base64("fn5-Pw=="), b"~~~?");
        assert_eq!(decode_base64("fn5+Pw"), b"~~~?");
        assert_eq!(decode_base64("not base64!"), b"");
    }

    #[test]
    fn counts_json_array_fields() {
        assert_eq!(json_array_len(br#"{"ackIds":["a","b"]}"#, "ackIds"), 2);
        assert_eq!(json_array_len(br#"{"ackIds":[]}"#, "ackIds"), 0);
        assert_eq!(json_array_len(br#"{}"#, "ackIds"), 0);
        assert_eq!(json_array_len(b"not json", "ackIds"), 0);
    }

    #[test]
    fn captures_payloads_and_attributes_from_a_publish_body() {
        let (sink, _rx) = crate::observe::test_sink();
        let proxy = RestProxy::new("localhost:8085".to_owned(), sink, 4);
        let messages = proxy.captured_messages(
            br#"{"messages":[
                    {"data":"aGVsbG8gcmVzdA==","attributes":{"src":"curl"}},
                    {"attributes":{}},
                    {"data":"aGk="}
                ]}"#,
        );

        assert_eq!(messages.len(), 3);
        // "hello rest" — 10 bytes, truncated to the 4-byte cap.
        assert_eq!(messages[0].data, b"hell");
        assert_eq!(messages[0].original_len, 10);
        assert!(messages[0].truncated);
        assert_eq!(
            messages[0].attributes,
            vec![("src".to_owned(), "curl".to_owned())]
        );
        // A message with no data at all is still a message, and still counted.
        assert_eq!(messages[1].original_len, 0);
        assert!(!messages[2].truncated);
        assert_eq!(messages[2].data, b"hi");
    }

    #[test]
    fn ignores_a_body_that_is_not_a_publish_request() {
        let (sink, _rx) = crate::observe::test_sink();
        let proxy = RestProxy::new("localhost:8085".to_owned(), sink, 64);
        assert!(proxy.captured_messages(b"<html>").is_empty());
        assert!(proxy.captured_messages(br#"{"messages":[]}"#).is_empty());
    }

    #[test]
    fn gunzips_an_encoded_body_and_passes_a_plain_one_through() {
        use std::io::Write as _;

        let mut headers = HeaderMap::new();
        assert_eq!(decoded(&headers, b"plain").as_deref(), Some(&b"plain"[..]));

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(b"squashed").expect("gzip a test body");
        let gzipped = encoder.finish().expect("finish gzipping a test body");

        headers.insert(header::CONTENT_ENCODING, "gzip".parse().expect("header"));
        assert_eq!(
            decoded(&headers, &gzipped).as_deref(),
            Some(&b"squashed"[..])
        );
        // An encoding we cannot undo leaves the body unobserved (but relayed).
        headers.insert(header::CONTENT_ENCODING, "br".parse().expect("header"));
        assert!(decoded(&headers, b"whatever").is_none());
    }

    #[test]
    fn strips_hop_by_hop_headers_but_keeps_the_rest() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, "proxy:8681".parse().expect("header"));
        headers.insert(header::CONNECTION, "keep-alive".parse().expect("header"));
        headers.insert(header::AUTHORIZATION, "Bearer t".parse().expect("header"));
        headers.insert(
            HeaderName::from_static("x-goog-request-params"),
            "topic=x".parse().expect("header"),
        );

        let upstream = relayed_headers(&headers, true);
        assert!(!upstream.contains_key(header::CONNECTION));
        assert!(!upstream.contains_key(header::HOST));
        assert_eq!(upstream[header::AUTHORIZATION], "Bearer t");
        assert_eq!(upstream["x-goog-request-params"], "topic=x");

        // Coming back the other way there is no host to drop.
        assert!(relayed_headers(&headers, false).contains_key(header::HOST));
    }
}
