use bytes::{BufMut, BytesMut};
use serde::Serialize;
use time::Timestamp;
use uuid::Uuid;

use crate::pdc_server::PDCServer;

#[derive(Debug)]
pub enum SensorProtocol {
    IEEEC37_118(PDCServer),
}

// impl SensorProtocol {
//     pub fn parse_data(&self, stream: BytesMut) {
//         match self {
//             SensorProtocol::IEEEC37_118 => {process_ieeec37(input_data)}
//         }
//     }
// }
//
// fn process_ieeec37(input_data: BytesMut) {}
// fn process_ieeec38(input_data: BytesMut) {}
// fn process_ieeec39(input_data: BytesMut) {}

#[derive(Debug)]
pub enum FrameType {
    DataFrame,
    HeaderFrame,
}

#[derive(Debug)]
pub struct Event {
    pub uuid: Uuid,
    pub buffer_value: f64,
    pub sensor: SensorProtocol,
    pub process_time: Timestamp,
}

impl Event {
    pub fn new(sensor: SensorProtocol, buffer_value: f64) -> Self {
        Self {
            uuid: Uuid::new_v4(),
            sensor,
            buffer_value,
            process_time: Timestamp::now(),
        }
    }
}

// Source: https://github.com/divi255/article-rust-storages/blob/main/src/main.rs
mod time {
    use serde::{Serialize, Serializer};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[derive(Copy, Clone, Debug)]
    pub struct Timestamp(Duration);

    impl Timestamp {
        pub fn now() -> Self {
            Self(SystemTime::now().duration_since(UNIX_EPOCH).unwrap())
        }
        pub fn before24h() -> Self {
            Self(Self::now().0 - Duration::from_secs(86400))
        }
        pub fn as_micros(self) -> u128 {
            self.0.as_micros()
        }
        pub fn as_nanos(self) -> u128 {
            self.0.as_nanos()
        }
    }
    impl Serialize for Timestamp {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_u128(self.as_nanos())
        }
    }
}
