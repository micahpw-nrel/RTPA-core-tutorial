use bytes::BytesMut;
use tokio::io::{self, AsyncReadExt};
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct TcpClient {
    pub stream: TcpStream,
    pub buffer: BytesMut,
}

impl TcpClient {
    pub async fn new(ip: String, port: u16) -> io::Result<Self> {
        let address = format!("{}:{}", ip, port);
        let stream = TcpStream::connect(address).await?;
        Ok(TcpClient {
            stream,
            buffer: BytesMut::with_capacity(2048),
        })
    }

    pub async fn read_data(&mut self) -> io::Result<()> {
        loop {
            let (mut socket, _) = self.stream.split();

            let bytes_read = socket.read_buf(&mut self.buffer).await?;
            if bytes_read == 0 {
                return Ok(());
            }
            println!(
                "Received {} bytes: {:?}",
                bytes_read,
                self.buffer.split_to(bytes_read)
            );
        }
    }
}
