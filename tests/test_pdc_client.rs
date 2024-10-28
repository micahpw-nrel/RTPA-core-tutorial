#![allow(unused)]
use arrow::array::{Array, Datum};
use arrow::ipc::reader::FileReader;
use bytes::Bytes;
use pmu::pdc_buffer_server;
use pmu::pdc_client::{ControlMessage, PDCClient};
use pmu::pdc_server::{run_mock_server, Protocol, ServerConfig};
use reqwest;
use std::io::Cursor;
use std::time::Duration;
use tokio::time;

#[tokio::test]
async fn test_pdc_client_server_communication() {
    // Start mock server in background
    println!("initializing server");

    let server_config =
        ServerConfig::new("127.0.0.1".to_string(), 4712, Protocol::TCP, 30.0).unwrap();

    let server_handle = tokio::spawn(async move {
        if let Err(e) = run_mock_server(server_config).await {
            println!("Mock server error: {}", e)
        };
    });

    // Give server time to start
    println!("Waiting for server to start");
    time::sleep(Duration::from_secs(1)).await;

    // Create and start client
    println!("Creating PDC Client...");
    let (mut pdc_client, control_rx, mut data_rx) =
        PDCClient::new("127.0.0.1", 4712, 1, Duration::from_secs(120))
            .await
            .expect("Failed to create PDC Client");

    let control_tx = pdc_client.get_control_sender();

    // Start client in background
    println!("Starting stream.");
    let client_handle = tokio::spawn(async move {
        println!("PDC client stream started");
        pdc_client.start_stream().await;
        println!("PDC client stream ended");
    });

    // Test sequence
    time::sleep(Duration::from_secs(2)).await;

    // Request buffer
    println!("Requesting buffer");
    control_tx.send(ControlMessage::GetBuffer).await.unwrap();

    // Check received data with timeout
    match tokio::time::timeout(Duration::from_secs(3), data_rx.recv()).await {
        Ok(Some(buffer)) => {
            println!("Received buffer of size: {}", buffer.len());

            assert!(!buffer.is_empty(), "Buffer should not be empty");
        }
        Ok(None) => panic!("Channel closed"),
        Err(_) => panic!("Timeout waiting for buffer"),
    }

    // Stop client
    println!("stopping client");
    control_tx.send(ControlMessage::Stop);

    // Clean up
    client_handle.abort();
    server_handle.abort();
}

#[tokio::test]
async fn test_buffer_server_data_endpoint() {
    // Start mock PDC server
    println!("Starting mock PDC server...");
    let pdc_server_config =
        ServerConfig::new("127.0.0.1".to_string(), 4712, Protocol::TCP, 30.0).unwrap();
    let pdc_server_handle = tokio::spawn(async move {
        if let Err(e) = run_mock_server(pdc_server_config).await {
            println!("Mock server error: {}", e);
        }
    });

    // Give PDC server time to start
    time::sleep(Duration::from_secs(1)).await;

    // Start buffer server
    println!("Starting buffer server...");
    std::env::set_var("PDC_HOST", "127.0.0.1");
    std::env::set_var("PDC_PORT", "4712");
    std::env::set_var("SERVER_PORT", "3000");

    let buffer_server_handle = tokio::spawn(async {
        if let Err(e) = pmu::pdc_buffer_server::run().await {
            println!("Buffer server error: {}", e);
        }
    });

    // Give buffer server time to start
    time::sleep(Duration::from_secs(2)).await;

    // Make request using reqwest
    println!("Making request to buffer server...");
    let client = reqwest::Client::new();
    let response = client
        .get("http://127.0.0.1:3000/data")
        .send()
        .await
        .expect("Failed to get response");

    assert!(response.status().is_success());

    // Get response bytes
    let arrow_buffer = response
        .bytes()
        .await
        .expect("Failed to get response bytes")
        .to_vec();
    let cursor = Cursor::new(&arrow_buffer);
    // Convert to Arrow RecordBatch
    let reader = FileReader::try_new(cursor, None).expect("Failed to create Arrow reader");

    // Print schema
    println!("\nSchema:");
    println!("{}", reader.schema());

    // Print first few rows
    println!("\nFirst few rows:");
    for batch in reader {
        let batch = batch.expect("Failed to read batch");
        println!("Number of rows: {}", batch.num_rows());

        for col_idx in 0..batch.num_columns() {
            println!(
                "{}: {:?} | ",
                batch.schema().field(col_idx).name(),
                batch.column(col_idx).slice(0, 5)
            );
        }
        println!();

        break; // Just print the first batch
    }

    // Clean up
    buffer_server_handle.abort();
    pdc_server_handle.abort();
}
