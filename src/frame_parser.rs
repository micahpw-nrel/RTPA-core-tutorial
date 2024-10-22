#![allow(unused)]

use crate::frames::{
    ConfigurationFrame1and2_2011, DataFrame2011, HeaderFrame2011, PMUConfigurationFrame2011,
};
use bincode::{deserialize, Error as BincodeError};
use crc::{Algorithm, Crc};

// Define constants
const COMMON_HEADER_SIZE: usize = 14; // Size of HeaderFrame2011 in bytes
const CONFIG_HEADER_SIZE: usize = 6;
const TAIL_SIZE: usize = 2; // Size of TailFrame2011 in bytes

// CRC_CCITT defenition
// Generating Polynomial: X^16+X^12+X^5+1
// Initial Value: -1 (hex FFFF)
// No final mask.
pub const CRC_CCITT_FALSE: Algorithm<u16> = Algorithm {
    width: 16,
    poly: 0x1021,
    init: 0xFFFF,
    refin: false,
    refout: false,
    xorout: 0x0000,
    check: 0x29B1,
    residue: 0x0000,
};

// Create a Crc instance with the custom algorithm
pub const CCITT_FALSE: Crc<u16> = Crc::<u16>::new(&CRC_CCITT_FALSE);

#[derive(Debug)]
pub enum ParseError {
    InsufficientData,
    DeserializationError(BincodeError),
    InvalidCRC,
    InvalidFrameSize,
    InvalidHeader,
    VersionNotSupported,
    NotImplemented,
}

#[derive(Debug)]
pub enum Frame {
    Header(HeaderFrame2011),
    Configuration(ConfigurationFrame1and2_2011),
    Data(DataFrame2011),
}

pub fn parse_header(buffer: &[u8]) -> Result<HeaderFrame2011, ParseError> {
    if buffer.len() < std::mem::size_of::<HeaderFrame2011>() {
        return Err(ParseError::InsufficientData);
    }

    deserialize(buffer).map_err(ParseError::DeserializationError)
}

pub fn parse_command_frame(buffer: &[u8]) -> Result<Frame, ParseError> {
    // TODO/skip
    todo!("Implement command frame parsing")
}

pub fn parse_data_frames(buffer: &[u8]) -> Result<DataFrame2011, ParseError> {
    // data frame parsing here.
    // TODO
    todo!("Implement data frame parsing")
}

pub fn parse_config_frame_1and2(buffer: &[u8]) -> Result<ConfigurationFrame1and2_2011, ParseError> {
    // Unsure about return type right now. Needs to be some sort
    // of nested structure.

    // get the header frame struct using the parse_header_frame function
    let common_header: HeaderFrame2011 = match parse_header(buffer) {
        Ok(header) => header,
        Err(e) => return Err(e),
    };

    // read the 4 bytes that come directly after the first 14 header bytes
    // into variable time_base. Read the next two bytes after that into num_pmu;
    let time_base: u32 = u32::from_be_bytes([
        buffer[COMMON_HEADER_SIZE],
        buffer[COMMON_HEADER_SIZE + 1],
        buffer[COMMON_HEADER_SIZE + 2],
        buffer[COMMON_HEADER_SIZE + 3],
    ]);
    let num_pmu: u16 = u16::from_be_bytes([
        buffer[COMMON_HEADER_SIZE + 4],
        buffer[COMMON_HEADER_SIZE + 5],
    ]);

    // Create a buffer offset variable that starts after
    // COMMMON_HEADER_SIZE and CONFIG_HEADER_SIZE
    // create a for loop based on config_header.num_pmu:
    // Read the next 26 bytes into a PMUConfigurationFrame2011 struct.
    // Increment the offset by 26 bytes.
    let mut offset = COMMON_HEADER_SIZE + 6;

    // PMU and Channel Configs should be same length.
    let mut pmu_configs = Vec::new();
    for _ in 0..num_pmu {
        if offset + 26 > buffer.len() {
            return Err(ParseError::InsufficientData);
        }
        // read the next 16 bytes into a variable pub stn [u8; 16]
        // increment offset by 16
        // read the next 2 bytes into a u16 variable idcode
        // read the next 2 bytes into a u16 variable format
        //
        // read the next 2 bytes into a u16 variable phnmr
        // read the next 2 bytes inot a u16 variable annmr
        let stn: [u8; 16] = buffer[offset..offset + 16].try_into().unwrap();
        offset += 16;
        let idcode = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
        offset += 2;
        let format = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
        offset += 2;
        let phnmr = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
        offset += 2;
        let annmr = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
        offset += 2;
        let dgnmr = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
        offset += 2;

        let mut pmu_config = PMUConfigurationFrame2011 {
            stn,
            idcode,
            format,
            phnmr,
            annmr,
            dgnmr,
            chnam: Vec::new(),   // Will be populated later
            phunit: Vec::new(),  // Will be populated later
            anunit: Vec::new(),  // Will be populated later
            digunit: Vec::new(), // Will be populated later
            fnom: 0,             // Will be populated later
            cfgcnt: 0,           // Will be populated later
        };

        // determine the next length of bytes to read populate the chnam field.
        let chnam_bytes_len = 16 * (phnmr + annmr + 16 * dgnmr) as usize;
        // read from offset to chname_bytes_len into a vec<u8> variable.
        let chnam = buffer[offset..offset + chnam_bytes_len].to_vec();
        offset += chnam_bytes_len as usize;
        pmu_config.chnam = chnam;

        // read from offset to 4*phnmr into a vec<u32> variable.
        let phunit = buffer[offset..offset + 4 * phnmr as usize]
            .chunks(4)
            .map(|chunk| u32::from_be_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<u32>>();
        offset += 4 * phnmr as usize;

        let anunit = buffer[offset..offset + 4 * annmr as usize]
            .chunks(4)
            .map(|chunk| u32::from_be_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<u32>>();
        offset += 4 * annmr as usize;

        let digunit = buffer[offset..offset + 4 * dgnmr as usize]
            .chunks(4)
            .map(|chunk| u32::from_be_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<u32>>();
        offset += 4 * dgnmr as usize;

        pmu_config.phunit = phunit;
        pmu_config.anunit = anunit;
        pmu_config.digunit = digunit;

        // finally read the next 2 bytes from the offset into fnom
        let fnom = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
        offset += 2;
        pmu_config.fnom = fnom;

        // read the next 2 bytes from the offset into cfgcnt
        let cfgcnt = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]);
        offset += 2;
        pmu_config.cfgcnt = cfgcnt;

        pmu_configs.push(pmu_config);
    }
    // Generate the configuration frame 1 and 2 based on the variables throughout this function.
    let config_frame = ConfigurationFrame1and2_2011 {
        header: common_header,
        time_base,
        num_pmu,
        pmu_configs,
        data_rate: i16::from_be_bytes([buffer[offset], buffer[offset + 1]]),
        chk: u16::from_be_bytes([buffer[offset + 2], buffer[offset + 3]]),
    };

    Ok(config_frame)
}

pub fn parse_config_frame_3(buffer: &[u8]) -> Result<Frame, ParseError> {
    // TODO
    todo!("Implement Config Frame type 3 parsing.")
}

pub fn parse_frame(buffer: &[u8]) -> Result<Frame, ParseError> {
    // read first two bytes as the sync variable.
    // read second byte and convert to binary
    // check bits 3-0 to get version number of IEEE standard.
    // if bits 3-0 == 0001, use IEEEstandard 2005
    // if bits 3-0 == 0010, use IEEE standard from 2011
    // if bits 3-0 do not equal 0010, throw ParseError:VersionNotSupported
    let sync = u16::from_be_bytes([buffer[0], buffer[1]]);
    if sync >> 8 != 0xAA {
        return Err(ParseError::InvalidHeader);
    }
    let version = buffer[1] & 0b1111;

    match version {
        1 => {
            // Use IEEE standard 2005
            return Err(ParseError::VersionNotSupported);
        }
        2 => {
            // Use IEEE standard 2011
            // No error
        }
        _ => return Err(ParseError::VersionNotSupported),
    }

    // Next, get framesize variable at bytes 3-4
    // verify framesize equals length of buffer.
    // verify checksum, CRC-CCITT matches check value
    // at framesize - 2 bytes
    let framesize = u16::from_be_bytes([buffer[2], buffer[3]]);
    if framesize as usize != buffer.len() {
        return Err(ParseError::InvalidFrameSize);
    }

    let calculated_crc = calculate_crc(&buffer[..buffer.len() - 2]);
    let frame_crc = u16::from_be_bytes([buffer[buffer.len() - 2], buffer[buffer.len() - 1]]);
    if calculated_crc != frame_crc {
        return Err(ParseError::InvalidCRC);
    }

    // convert second byte of sync variable to bit representation.
    // If bits 6-4 equal 000 -> parse_data_frame (buffer: &[u8], framesize:u16)
    // if bits 6-4 equal 001 -> parse_header_frame (buffer. framesize)
    // if bits 6-4 equal 010 or 011 -> parse_config_frame_1and2(buffer, framesize)
    // If bits 6-4 equal 101 -> parse_config_frame_3(buffer, framesize)
    // if bits 6-4 equal 100 -> parse_command_frame(buffer, framesize)
    let frame_type = (buffer[1] >> 4) & 0b111;
    match frame_type {
        0b000 => {
            let data_frame = parse_data_frames(buffer)?;
            Ok(Frame::Data(data_frame))
        }
        0b001 => {
            let header = parse_header(buffer)?;
            Ok(Frame::Header(header))
        }
        0b010 | 0b011 => {
            let config = parse_config_frame_1and2(buffer)?;
            Ok(Frame::Configuration(config))
        }
        0b101 => parse_config_frame_3(buffer),
        0b100 => parse_command_frame(buffer),
        _ => Err(ParseError::InvalidFrameSize),
    }
}

pub fn calculate_crc(data: &[u8]) -> u16 {
    CCITT_FALSE.checksum(data)
}
