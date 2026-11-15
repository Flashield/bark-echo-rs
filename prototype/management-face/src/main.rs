//! Ticket #23 prototype: one web management page to react to.
//!
//! A throwaway, self-contained axum 0.8 crate serving the page the way
//! the real daemon will serve it (ADR-0005): server-rendered HTML shell
//! plus a thin vanilla-JS consumer of JSON endpoints, assets embedded
//! with `include_str!`, no node build chain. Mock data only — the
//! simulator in `mock.rs` stands in for the daemon's streams.
//!
//! Run: `cargo run` in this directory, then open http://localhost:8081
//! (from a phone on the same network: http://<this-host>:8081). Demo
//! management token: `demo`. The bind is `0.0.0.0` on purpose — the
//! mock serves no secrets, so the LAN demo exposure is fine; the real
//! daemon defaults to `127.0.0.1:8081` with LAN bind as an explicit
//! opt-in (#24).

mod api;
mod mock;

use axum::Router;
use axum::routing::{delete, get, post};

use crate::mock::MockState;

#[tokio::main]
async fn main() {
    let state = MockState::new();
    mock::spawn_simulator(state.clone());

    let app = Router::new()
        .route("/", get(api::index))
        .route("/assets/style.css", get(api::style))
        .route("/assets/app.js", get(api::script))
        .route("/v1/session", post(api::session))
        .route("/v1/status", get(api::status))
        .route("/v1/events", get(api::events))
        .route("/v1/subscriptions", post(api::subscribe_add))
        .route("/v1/subscriptions/{id}", delete(api::subscribe_remove))
        .route("/v1/log/{id}", get(api::log_tail))
        .route("/v1/push", post(api::push))
        .with_state(state);

    // 8081: bark-server owns 8080; the real face takes this port by
    // default (#24). Fail loud on a squatted port rather than picking
    // another one silently.
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8081")
        .await
        .unwrap_or_else(|err| panic!("cannot bind 0.0.0.0:8081 (port squatted?): {err}"));
    println!("management-face prototype (ticket #23) listening on http://0.0.0.0:8081");
    println!("  local:  http://localhost:8081");
    println!("  phone:  http://<this-host>:8081   (same network)");
    println!("  token:  demo   (POST /v1/session exchanges it for the session cookie)");
    println!("  mock data only — a simulator fakes pushes and stream flips every few seconds");

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .expect("server error");
}
