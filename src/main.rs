#![allow(unused)]
mod client;
mod errors;
mod pdc_server;
mod phasor;
mod frames;
//mod server;
use circular_buffer::CircularBuffer;
use clap::{Args, Parser, Subcommand, ValueEnum};
use client::TcpClient;
use log::{info, warn};
use pdc_server::{run_mock_server, PDCServer, Protocol, ServerConfig};
use tokio::io;
#[derive(Debug, Parser)] // requires `derive` feature
#[command(name = "pmu")]
#[command(about = "Testing PMU", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Server {
        #[arg(default_value = "127.0.0.1")]
        ip: String,
        #[arg(default_value_t = 8080)]
        port: u16,
    },
    #[command(arg_required_else_help = true)]
    Client { ip: String, port: u16, freq: f64 },
    #[command(arg_required_else_help = true)]
    Data { query: String },
}

#[tokio::main]
async fn main() -> io::Result<()> {
    env_logger::init();
    info!("Starting application");

    let args = Cli::parse();

    match args.command {
        Commands::Server { ip, port } => {
            println!("Using {ip} and port {port}");
            let server_config = ServerConfig::new(ip, port, Protocol::TCP).unwrap();
            let freq = 1.; // 1 Hz
            let server = PDCServer::new(freq, 10).unwrap();
            println!("{:#?}", server);
            run_mock_server(server, server_config).await;
        }
        Commands::Client { ip, port, freq } => {
            let server = PDCServer::new(freq, 10).unwrap();

            // Create buffer depending on the server configuration
            // let mut buf = CircularBuffer::<60, u32>::new();
            let mut client = TcpClient::new(ip, port).await.unwrap();
            client.read_data().await?;
            println!("{:#?}", &client);
        }
        Commands::Data { query } => {}
    }
    Ok(())
}
