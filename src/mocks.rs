use crate::frame_parser::calculate_crc;
use crate::frames::{
    ConfigurationFrame1and2_2011, DataFrame2011, HeaderFrame2011, PMUConfigurationFrame2011,
};

pub fn mock_header_frame() -> Vec<u8> {
    let header = HeaderFrame2011 {
        sync: [0xAA, 0x01],
        framesize: 14,
        idcode: 1,
        soc: 1234567890,
        fracsec: 0,
    };
    bincode::serialize(&header).unwrap()
}

pub fn mock_config_frame() -> Vec<u8> {
    let config = ConfigurationFrame1and2_2011 {
        header: HeaderFrame2011 {
            sync: [0xAA, 0x31],
            framesize: 0, // Will be updated later
            idcode: 1,
            soc: 1234567890,
            fracsec: 0,
        },
        time_base: 1000000,
        num_pmu: 1,
        pmu_configs: vec![PMUConfigurationFrame2011 {
            stn: *b"STATION1        ",
            idcode: 1,
            format: 0,
            phnmr: 1,
            annmr: 1,
            dgnmr: 1,
            chnam: b"CH1             CH2             CH3             ".to_vec(),
            phunit: vec![0],
            anunit: vec![0],
            digunit: vec![0],
            fnom: 60,
            cfgcnt: 0,
        }],
        data_rate: 30,
        chk: 0, // Will be updated later
    };
    let mut frame = bincode::serialize(&config).unwrap();
    let framesize = frame.len() as u16;
    frame[2..4].copy_from_slice(&framesize.to_be_bytes());
    let frame_len = frame.len();
    let crc = calculate_crc(&frame[..frame_len - 2]);
    frame[frame_len - 2..].copy_from_slice(&crc.to_be_bytes());
    frame
}

pub fn mock_data_frame() -> Vec<u8> {
    let data = DataFrame2011 {
        stat: 0,
        phasors: 1000,
        freq: 60000,
        dfreq: 100,
        analog: 500,
        digital: 1,
    };
    let header = HeaderFrame2011 {
        sync: [0xAA, 0x01],
        framesize: 0, // Will be updated later
        idcode: 1,
        soc: 1234567890,
        fracsec: 0,
    };
    let mut frame = bincode::serialize(&header).unwrap();
    frame.extend(bincode::serialize(&data).unwrap());
    let framesize = frame.len() as u16 + 2; // +2 for CRC
    frame[2..4].copy_from_slice(&framesize.to_be_bytes());
    let crc = calculate_crc(&frame);
    frame.extend(&crc.to_be_bytes());
    frame
}
