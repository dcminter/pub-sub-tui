//! Boilerplate-reducing macro for the proxy's service implementations.
//!
//! Most Pub/Sub RPCs carry no traffic we want to count; the proxy just relays
//! them to the upstream server and returns the response (and any `Status`)
//! verbatim, so the client behaves exactly as if it had talked to the server
//! directly. This macro generates those trivially-forwarding methods alongside
//! the hand-written, observed ones.
//!
//! The macro emits the `#[tonic::async_trait]` attribute itself: the trait is
//! declared with `#[async_trait]`, so every method (generated or custom) must be
//! desugared by `async_trait`. Because the attribute is part of the macro's
//! output, it runs *after* expansion and therefore sees the generated methods —
//! which it would not if it were written outside a macro invocation.

/// Implement a proxy service: forward the listed unary RPCs to the upstream
/// client returned by `self.$accessor()`, and splice in the `custom { … }`
/// methods (and any associated types) verbatim.
macro_rules! proxy_service {
    (
        service = $service:path;
        proxy = $proxy:ty;
        accessor = $accessor:ident;
        forward { $( $method:ident($req:ty) -> $resp:ty );* $(;)? }
        custom { $($custom:tt)* }
    ) => {
        #[tonic::async_trait]
        impl $service for $proxy {
            $($custom)*

            $(
                async fn $method(
                    &self,
                    request: tonic::Request<$req>,
                ) -> std::result::Result<tonic::Response<$resp>, tonic::Status> {
                    let result = self.$accessor().$method(request).await;
                    if let Err(status) = &result {
                        tracing::warn!(
                            rpc = stringify!($method),
                            code = ?status.code(),
                            message = status.message(),
                            "upstream RPC returned error status",
                        );
                    }
                    result
                }
            )*
        }
    };
}

pub(crate) use proxy_service;
