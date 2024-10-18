//use bincode::Deserializer;
use std::io;
#[cfg(test)]
mod tests {
    use pmu::frames::HeaderFrame2011;
    use pmu::mocks::{mock_headerdata, mock_simple};
    #[test]
    fn test_mock_data() {
        let x = mock_simple();
        assert_eq!(x, 1)
    }

    #[test]
    fn test_header_parse() {
        let mock_header = mock_headerdata();
        let header_decoded: HeaderFrame2011 = bincode::deserialize(&mock_header).unwrap();

        assert_eq!(header_decoded.fracsec, 456);
    }
}
