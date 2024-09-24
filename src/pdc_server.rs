const BUFFER_MAX_SIZE: u32 = 104857600;
const SECONDS_PER_MINUTE: u32 = 60;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::io::{self, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{self, Duration};

#[derive(Debug)]
pub enum Protocol {
    TCP,
    UDP,
}

#[derive(Debug)]
pub struct ServerConfig {
    pub ip: String,
    pub port: u16,
    pub protocol: Protocol,
    pub address: String,
}

impl ServerConfig {
    pub fn new(ip: String, port: u16, protocol: Protocol) -> Result<Self, String> {
        if let Protocol::UDP = protocol {
            return Err("UDP is not implemented".to_string());
        }
        let address = format!("{}:{}", ip, port);
        Ok(ServerConfig {
            ip,
            port,
            protocol,
            address,
        })
    }
}

#[derive(Debug)]
pub struct PDCServer {
    pub freq: f64, // Frequency should be in Hertz
    pub no_pmus: usize,
    pub buffer_size: usize,
}

impl PDCServer {
    pub fn new(freq: f64, no_pmus: usize) -> Result<Self, String> {
        if freq <= 0.0 {
            return Err("Frequency must be greater than 0".to_string());
        }

        // Store data from any input stream on the buffer with the lowest denominator
        //
        let buffer_size = (60.0 / freq).ceil() as usize;
        Ok(PDCServer {
            freq,
            no_pmus,
            buffer_size,
        })
    }
}

pub async fn run_mock_server(pdc_server: PDCServer, server_config: ServerConfig) -> io::Result<()> {
    let listener = TcpListener::bind(&server_config.address).await?;
    println!("Mock server listening on {}", server_config.address);

    let frequency = Duration::from_secs(pdc_server.freq as u64); // 1 second
    loop {
        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            // let mut rng = StdRng::from_entropy();
            loop {
                // let random_bytes: Vec<u8> = (0..10).map(|_| rng.gen()).collect();
                let random_bytes = mock_ieeec37data();
                if let Err(e) = socket.write_all(&random_bytes).await {
                    println!("Failed to write to socket: {}", e);
                    return;
                }
                time::sleep(frequency).await;
            }
        });
    }
}

fn mock_ieeec37data() -> Vec<u8> {
    // Header Frame
    // b'\xaaA\x00\x12\x00\x07f\xf3,0\x00\x00\xa0\xa5\x00\x03\xfa\x17'
    let dummy_data = &[0x41u8, 0x41u8, 0x42u8];
    return Vec::from(dummy_data);
}
