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
//#![allow(unused)]
use crate::arrow_utils::{build_arrow_schema, extract_channel_values};
use crate::frames::ConfigurationFrame1and2_2011;
use crate::pdc_client::{ControlMessage, PDCClient};
use arrow::array::ArrayRef;
use arrow::array::TimestampMicrosecondArray;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Router};
use bytes::Bytes;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

// Basic configuration structure
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
                .unwrap_or_else(|_| "8123".to_string())
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
                .unwrap_or_else(|_| "8080".to_string())
                .parse()
                .map_err(|_| "Invalid SERVER_PORT")?,
        })
    }
}

// Application state
#[derive(Clone)]
struct AppState {
    control_tx: mpsc::Sender<ControlMessage>,
    //data_rx: mpsc::Receiver<Vec<u8>>,
    config: ConfigurationFrame1and2_2011,
    frame_size: usize,
}

// Response for configuration endpoint
//async fn get_config(
//State(state): State<AppState>,
//) -> Result<Json<ConfigurationFrame1and2_2011>, StatusCode> {
//let pdc_client = state
//    .pdc_client
//    .lock()
//        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

//let config = match &pdc_client.config {
//Some(config) => config,
//None => return Err(StatusCode::NOT_FOUND),
//};

//config.map(Json)
//StatusCode::NOT_IMPLEMENTED
//}

// Response for buffer data endpoint
async fn get_buffer_data(
    State(state): State<AppState>,
    data_rx: Arc<Mutex<mpsc::Receiver<Vec<u8>>>>,
) -> Result<impl IntoResponse, StatusCode> {
    // Send request for buffer
    state
        .control_tx
        .send(ControlMessage::GetBuffer)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    // Wait for response
    let buffer = {
        let mut rx = data_rx.lock().await;
        tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .map_err(|_| StatusCode::REQUEST_TIMEOUT)?
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?
    };
    // Get channel map from config
    let channel_map = state.config.get_channel_map();

    // Build Arrow schema
    let schema = Arc::new(build_arrow_schema(&channel_map));

    // Create arrays for each channel
    let mut arrays: Vec<ArrayRef> = Vec::new();

    // Add timestamp array
    let mut timestamps = Vec::new();

    for frame in buffer.chunks(state.frame_size) {
        if frame.len() >= 14 {
            let soc = u32::from_be_bytes([frame[6], frame[7], frame[8], frame[9]]);
            let fracsec = u32::from_be_bytes([frame[10], frame[11], frame[12], frame[13]]);
            timestamps.push((soc as i64) * 1_000_000 + (fracsec as i64));
        }
    }
    arrays.push(Arc::new(TimestampMicrosecondArray::from(timestamps)));

    // Extract values for each channel
    for (_, info) in &channel_map {
        let channel_arrays = extract_channel_values(&buffer, state.frame_size, info);
        arrays.extend(channel_arrays);
    }

    // Create RecordBatch
    let record_batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Serialize to Arrow IPC format
    let mut buf = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut buf, &schema)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        writer
            .write(&record_batch)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        writer
            .finish()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/arrow-ipc")],
        Bytes::from(buf),
    ))
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration
    let config = Config::from_env()?;

    // Initialize PDC client
    let (mut pdc_client, control_tx, data_rx) = match PDCClient::new(
        &config.pdc_host,
        config.pdc_port,
        config.pdc_idcode,
        config.buffer_duration,
    )
    .await
    {
        Ok(client_tuple) => client_tuple,
        Err(e) => {
            eprintln!("Failed to connect to PDC server: {}", e);
            // Exit the process with error code
            std::process::exit(1);
        }
    };
    // Start PDC client in background
    // Get initial configuration

    let frame_size = pdc_client.frame_size;
    let pdc_config = pdc_client
        .get_config()
        .expect("No Configuration Frame, shutting down.");

    let _client_handle = tokio::spawn(async move {
        println!("PDC client stream started");
        pdc_client.start_stream().await;
        println!("PDC client stream ended");
        std::process::exit(1);
    });

    // Create application state
    let app_state = AppState {
        control_tx,
        config: pdc_config,
        frame_size,
    };

    // Create a shared data receiver
    let data_rx = Arc::new(Mutex::new(data_rx));
    let data_rx_clone = data_rx.clone();

    // Build router with access to data_rx
    let app = Router::new()
        //.route("/config", get(get_config))
        .route(
            "/data",
            get(move |state| get_buffer_data(state, data_rx_clone.clone())),
        )
        .with_state(app_state);

    // Start server
    let addr = SocketAddr::from(([127, 0, 0, 1], config.server_port));
    println!("Server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    println!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
