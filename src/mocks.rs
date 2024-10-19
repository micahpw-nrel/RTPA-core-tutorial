use crate::frames::{
    ConfigurationFrame1And2_2011, DataFrame2011, HeaderFrame2011, HeaderFrame2024, TailFrame2011,
};
use rand::Rng;

pub fn mock_header2024() -> Vec<u8> {
    let header = HeaderFrame2024 {
        sync: [0xAA, 0x02],
        framesize: 2000,
        stream_id: 1234,
        soc: 1621234567,
        leap_byte: 0,
        fracsec: [0x01, 0x23, 0x45],
    };
    bincode::serialize(&header).unwrap()
}

pub fn mock_dataframe2011() -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let data = DataFrame2011 {
        stat: rng.gen(),
        phasors: rng.gen(),
        freq: rng.gen(),
        dfreq: rng.gen(),
        analog: rng.gen(),
        digital: rng.gen(),
    };
    bincode::serialize(&data).unwrap()
}

pub fn mock_configframe1and2_2011() -> Vec<u8> {
    let config = ConfigurationFrame1And2_2011 {};
    bincode::serialize(&config).unwrap()
}

pub fn mock_tailframe2011() -> Vec<u8> {
    let tail = TailFrame2011 { chk: 0xABCD };
    bincode::serialize(&tail).unwrap()
}

pub fn mock_complete_frame() -> Vec<u8> {
    let mut frame = Vec::new();
    frame.extend(mock_header2011());
    frame.extend(mock_dataframe2011());
    frame.extend(mock_tailframe2011());
    frame
}

pub fn mock_invalid_frame() -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..100).map(|_| rng.gen::<u8>()).collect()
}

pub fn mock_header2011() -> Vec<u8> {
    let header = HeaderFrame2011 {
        sync: [1, 2],
        framesize: 1234,
        idcode: 2345,
        soc: 123,
        fracsec: 456,
    };
    let header_serial = bincode::serialize(&header).unwrap();
    //let dummy_data = &[0x41u8, 0x41u8, 0x42u8];
    return header_serial;
}
