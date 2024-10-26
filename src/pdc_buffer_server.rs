// This code is for creating an http server for end users
//
// It should start a pdc_client that buffers data frames from an
// upstream pdc server.
//
// It should allow users to request the entire buffer,
// or last N seconds of buffer data.
//
// It should parse the buffer data into the a dataframe based
// on the IEEE C37.118.2 standard.
//
// It should return that data as arrow dataframes over IPC.
//
// What it shouldn't do. (for now)
// Send configuration commands to the upstream pdc server.
//
#![allow(unused)]
use std::env;
use std::net::SocketAddr;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
//use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::frame_parser::parse_frame;
use crate::frames::{ChannelDataType, ChannelInfo, ConfigurationFrame1and2_2011};
use crate::pdc_client::{ControlMessage, PDCClient};

// Environment configuration struct
#[derive(Debug)]
struct Config {
    pdc_host: String,
    pdc_port: u16,
    pdc_idcode: u16,
    buffer_duration: Duration,
    server_port: u16,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        Ok(Config {
            pdc_host: env::var("PDC_HOST").unwrap_or_else(|_| "localhost".to_string()),
            pdc_port: env::var("PDC_PORT")
                .unwrap_or_else(|_| "4712".to_string())
                .parse()
                .map_err(|_| "Invalid PDC_PORT")?,
            pdc_idcode: env::var("PDC_IDCODE")
                .unwrap_or_else(|_| "1".to_string())
                .parse()
                .map_err(|_| "Invalid PDC_IDCODE")?,
            buffer_duration: Duration::from_secs(
                env::var("BUFFER_DURATION_SECS")
                    .unwrap_or_else(|_| "120".to_string())
                    .parse()
                    .map_err(|_| "Invalid BUFFER_DURATION_SECS")?,
            ),
            server_port: env::var("SERVER_PORT")
                .unwrap_or_else(|_| "3000".to_string())
                .parse()
                .map_err(|_| "Invalid SERVER_PORT")?,
        })
    }
}

// Application state
#[derive(Clone)]
struct AppState {
    control_tx: mpsc::Sender<ControlMessage>,
    data_rx: Arc<Mutex<mpsc::Receiver<Vec<u8>>>>,
}

// Response types
#[derive(Serialize)]
struct BufferResponse {
    frames: Vec<DataFrame2011>,
    timestamp: String,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

// Request handlers
async fn get_buffer(
    State(state): State<AppState>,
) -> Result<Json<BufferResponse>, (StatusCode, Json<ErrorResponse>)> {
    let pdc_client = state.pdc_client.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Failed to acquire lock".to_string(),
            }),
        )
    })?;

    let buffer = pdc_client.get_buffer_contents(None);
    let frames = parse_buffer_to_frames(&buffer, &pdc_client).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;

    Ok(Json(BufferResponse {
        frames,
        timestamp: chrono::Utc::now().to_rfc3339(),
    }))
}

async fn get_last_n_seconds(
    State(state): State<AppState>,
    Path(seconds): Path<u64>,
) -> Result<Json<BufferResponse>, (StatusCode, Json<ErrorResponse>)> {
    let pdc_client = state.pdc_client.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Failed to acquire lock".to_string(),
            }),
        )
    })?;

    let duration = Duration::from_secs(seconds);
    let buffer = pdc_client.get_buffer_contents(Some(duration));
    let frames = parse_buffer_to_frames(&buffer, &pdc_client).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;

    Ok(Json(BufferResponse {
        frames,
        timestamp: chrono::Utc::now().to_rfc3339(),
    }))
}

// Helper function to parse buffer into frames
fn parse_buffer_to_frames(
    buffer: &[Vec<u8>],
    pdc_client: &PDCClient,
) -> Result<Vec<DataFrame2011>, String> {
    let mut frames = Vec::new();

    for frame_data in buffer {
        if let Ok(frame) = parse_frame(frame_data, pdc_client.config().clone()) {
            if let Frame::Data(data_frame) = frame {
                frames.push(data_frame);
            }
        }
    }

    Ok(frames)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load configuration from environment
    let config = Config::from_env().map_err(|e| {
        eprintln!("Configuration error: {}", e);
        std::process::exit(1);
    })?;

    // Initialize PDC client
    let pdc_client = PDCClient::new(
        &config.pdc_host,
        config.pdc_port,
        config.pdc_idcode,
        config.buffer_duration,
    )
    .map_err(|e| {
        eprintln!("Failed to initialize PDC client: {}", e);
        std::process::exit(1);
    })?;

    // Start PDC client stream
    let pdc_client = Arc::new(Mutex::new(pdc_client));
    let pdc_client_clone = Arc::clone(&pdc_client);

    tokio::spawn(async move {
        let mut client = pdc_client_clone.lock().unwrap();
        client.start_stream();
    });

    // Create router with application state
    let app_state = AppState { pdc_client };

    let app = Router::new()
        .route("/buffer", get(get_buffer))
        .route("/buffer/:seconds", get(get_last_n_seconds))
        .layer(TraceLayer::new_for_http())
        .with_state(app_state);

    // Build server address
    let addr = SocketAddr::from(([127, 0, 0, 1], config.server_port));
    tracing::info!("listening on {}", addr);

    // Start server
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}
