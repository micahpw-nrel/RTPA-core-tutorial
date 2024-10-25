// This module is for connecting to a pdc server,
// getting the configuration frame and header frames.
// starting a data stream and storing the results in a ring buffer
// stopping the data stream and shutting down.
//
// It uses a background thread to constantly read from the PDC server,
// allowing the main thread to grab copies of the buffer when needed.
#![allow(unused)]
use std::collections::VecDeque;
use std::io::prelude::*;
use std::net::TcpStream;
use std::sync::mpsc;
use std::time::{Duration, SystemTime};

use crate::{
    frame_parser::parse_config_frame_1and2,
    frames::{calculate_crc, CommandFrame2011, ConfigurationFrame1and2_2011, PrefixFrame2011},
};

// Define an enum to represent different buffer types
enum BufferType {
    Stack([u8; 30 * 1024]),                // 30KB stack buffer
    Heap(VecDeque<(SystemTime, Vec<u8>)>), // Heap buffer with timestamps
}

pub struct PDCClient {
    stream: TcpStream,
    //allocate 30 kB to the stack to serve as a ring buffer.
    buffer: BufferType,
    buffer_duration: Duration,
    frame_size: usize,
    write_offset: usize,
    idcode: u16,
    tx: mpsc::Sender<()>,
    rx: mpsc::Receiver<()>,
    config: Option<ConfigurationFrame1and2_2011>,
}

impl PDCClient {
    // TODO update to allow creation of pdc client with host, port and idcode.
    pub fn new(
        host: &str,
        port: u16,
        idcode: u16,
        buffer_duration: Duration,
    ) -> Result<Self, std::io::Error> {
        let stream = TcpStream::connect(format!("{}:{}", host, port))?;
        let (tx, rx) = mpsc::channel();

        let mut client = PDCClient {
            stream,
            buffer: BufferType::Stack([0; 30 * 1024]),
            buffer_duration,
            frame_size: 0,
            write_offset: 0,
            idcode,
            tx,
            rx,
            config: None,
        };

        // Get initial configuration
        client.get_config_frame()?;
        client.initialize_buffer()?;

        Ok(client)
    }
    fn verify_frame_crc(&self, buffer_data: &[u8]) -> bool {
        todo!("Calculate the crc value based on the frame - last 2 bytes");
    }
    fn calculate_frame_size(&self) -> usize {
        // Calculate frame size based on configuration
        // This will depend on your specific PMU configuration
        let frame_size = if let Some(ref config) = self.config {
            // Start with the common frame size (prefix + chk)
            let mut size = 14 + 2;

            for pmu_config in &config.pmu_configs {
                // Add size for STAT word
                size += 2;

                // Add size for phasors (format determines whether fixed or float)
                size += pmu_config.phasor_size() * pmu_config.phnmr as usize;

                // Add size for FREQ and DFREQ
                size += 2 * pmu_config.freq_dfreq_size();

                // Add size for analog values
                size += pmu_config.analog_size() * pmu_config.annmr as usize;

                // Add size for digital words
                size += 2 * pmu_config.dgnmr as usize;
            }
            size
        } else {
            0
        };

        frame_size
    }

    fn get_next_offset(&self, buffer_size: usize) -> usize {
        // Implement circular buffer offset calculation
        todo!("Implement circular buffer offset calculation")
    }
    fn initialize_buffer(&mut self) -> Result<(), std::io::Error> {
        if let Some(ref config) = self.config {
            // Calculate expected frame size based on configuration
            let frame_size = self.calculate_frame_size();
            self.frame_size = frame_size;

            // Calculate required buffer size based on data rate and buffer duration
            let data_rate = config.data_rate as f64;
            let frames_per_second = data_rate.abs();
            let total_frames = (frames_per_second * self.buffer_duration.as_secs_f64()) as usize;
            let required_size = frame_size * total_frames;

            // Switch to heap buffer if required size is too large
            if required_size > 30 * 1024 {
                self.buffer = BufferType::Heap(VecDeque::with_capacity(total_frames));
                println!(
                    "Using heap buffer with capacity for {} frames",
                    total_frames
                );
            } else {
                println!("Using stack buffer");
            }
        }
        Ok(())
    }
    fn get_config_frame(&mut self) -> Result<(), std::io::Error> {
        // Send a command frame to get the configuration of the PDC server

        let cmd = CommandFrame2011::new_send_config_frame1(self.idcode);
        self.stream.write_all(&cmd.to_hex())?;

        // Read and parse configuration response
        let mut buf = [0u8; 1024];
        let n = self.stream.read(&mut buf)?;

        if let Ok(config) = parse_config_frame_1and2(&buf[..n]) {
            self.config = Some(config);
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to get configuration",
            ))
        }

        //let config_frame = CommandFrame2011::new_send_config_frame1(self.idcode);
        //let config_frame_bytes = config_frame.to_hex();
        //self.stream.write_all(&config_frame_bytes).unwrap();
    }

    pub fn start_stream(mut self) {
        // TODO we should add some code to send two command frames.
        // 1. Get the configuration frame.
        // 2. Start the Data Stream.

        std::thread::spawn(move || {
            self.receive_loop();
        });
    }

    pub fn stop_stream(&self) {
        self.tx.send(()).unwrap(); // Tell the background thread to stop
    }

    fn receive_loop(&mut self) {
        let mut temp_buffer = Vec::new();

        loop {
            let mut buf = [0u8; 1024];

            // Check for stop signal
            match self.rx.try_recv() {
                Ok(_) | Err(mpsc::TryRecvError::Disconnected) => {
                    let stop_cmd = CommandFrame2011::new_turn_off_transmission(self.idcode);
                    self.stream.write_all(&stop_cmd.to_hex()).unwrap();
                    break;
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }

            match self.stream.read(&mut buf) {
                Ok(n) if n >= 14 => {
                    // Read prefix to get frame size
                    let prefix_slice: &[u8; 14] = &buf[..14].try_into().unwrap();
                    if let Ok(prefix) = PrefixFrame2011::from_hex(prefix_slice) {
                        let frame_size = prefix.framesize as usize;

                        if n >= frame_size {
                            // Complete frame received
                            let frame_data = &buf[..frame_size];
                            if self.verify_frame_crc(frame_data) {
                                self.store_frame(frame_data);
                            }
                        } else {
                            // Partial frame, collect in temp buffer
                            temp_buffer.extend_from_slice(&buf[..n]);
                            if temp_buffer.len() >= frame_size {
                                if self.verify_frame_crc(&temp_buffer[..frame_size]) {
                                    self.store_frame(&temp_buffer[..frame_size]);
                                }
                                temp_buffer.clear();
                            }
                        }
                    }
                }
                Ok(_) | Err(_) => {
                    eprintln!("Error reading from stream");
                    break;
                }
            }
        }
    }
    // End of Receive loop.
    //
    fn store_frame(&mut self, frame_data: &[u8]) {
        match &mut self.buffer {
            BufferType::Stack(buffer) => {
                // Check if frame fits at current offset
                if self.write_offset + self.frame_size > buffer.len() {
                    // Not enough space at end, start from beginning
                    self.write_offset = 0;
                }

                // Copy frame data to buffer
                buffer[self.write_offset..self.write_offset + self.frame_size]
                    .copy_from_slice(frame_data);

                // Update write offset
                self.write_offset += self.frame_size;
            }
            BufferType::Heap(buffer) => {
                let now = SystemTime::now();
                buffer.push_back((now, frame_data.to_vec()));

                // Remove old frames based on buffer duration
                while let Some((timestamp, _)) = buffer.front() {
                    if now.duration_since(*timestamp).unwrap() > self.buffer_duration {
                        buffer.pop_front();
                    } else {
                        break;
                    }
                }
            }
        }
    }
    pub fn get_buffer_contents(&self, duration: Option<Duration>) -> Vec<Vec<u8>> {
        match &self.buffer {
            BufferType::Stack(buffer) => {
                // Return all frames from stack buffer
                // You'll need to implement frame parsing logic here
                vec![buffer.to_vec()]
            }
            BufferType::Heap(buffer) => {
                // Filter frames based on duration if specified
                let threshold = duration.map(|d| SystemTime::now() - d);
                buffer
                    .iter()
                    .filter(|(timestamp, _)| threshold.map_or(true, |t| *timestamp >= t))
                    .map(|(_, frame)| frame.clone())
                    .collect()
            }
        }
    }
}
