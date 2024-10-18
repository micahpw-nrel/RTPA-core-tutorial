use crate::frames::HeaderFrame2011;
//use serde::{Serialize};
pub fn mock_simple() -> u8 {
    return 1;
}
// Want to mock the
pub fn mock_headerdata() -> Vec<u8> {
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
