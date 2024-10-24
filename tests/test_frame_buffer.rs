#[cfg(test)]
mod tests {
    use super::*;
    use pmu::frame_buffer::{ColumnData, ColumnType, DataSlice, PMUDataStore, PMUValue};
    use pmu::frame_parser::{parse_config_frame_1and2, parse_data_frames};
    use pmu::frames::{ConfigurationFrame1and2_2011, PMUFrameType, PMUValues};
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;

    fn read_hex_file(file_name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let path = Path::new("tests/test_data").join(file_name);
        let content = fs::read_to_string(path)?;
        let hex_string: String = content.chars().filter(|c| !c.is_whitespace()).collect();

        hex_string
            .as_bytes()
            .chunks(2)
            .map(|chunk| {
                let hex_byte = std::str::from_utf8(chunk).unwrap();
                u8::from_str_radix(hex_byte, 16).map_err(|e| e.into())
            })
            .collect()
    }

    #[test]
    fn test_basic_buffer_operations() {
        let mut store = PMUDataStore::new(3, 100);
        store.add_column("phasor1".to_string(), ColumnType::Phasor);
        store.add_column("analog1".to_string(), ColumnType::Analog);
        store.add_column("digital1".to_string(), ColumnType::Digital);

        assert!(store.columns.contains_key("phasor1"));
        assert!(store.columns.contains_key("analog1"));
        assert!(store.columns.contains_key("digital1"));
    }

    #[test]
    fn test_data_insertion_and_retrieval() {
        // Parse config frame
        let config_buffer = read_hex_file("config_message.bin").unwrap();
        let config_frame = parse_config_frame_1and2(&config_buffer).unwrap();

        // Create data store
        let mut data_store = PMUDataStore::new(10, 3600);

        // Add columns based on first PMU config
        let pmu_config = &config_frame.pmu_configs[0];
        let column_names = pmu_config.get_column_names();

        for (i, name) in column_names.iter().enumerate() {
            if i < pmu_config.phnmr as usize {
                data_store.add_column(name.clone(), ColumnType::FixedPhasor);
            } else if i < (pmu_config.phnmr + pmu_config.annmr) as usize {
                data_store.add_column(name.clone(), ColumnType::Analog);
            } else {
                data_store.add_column(name.clone(), ColumnType::Digital);
            }
        }

        // Parse and insert data frame multiple times
        let data_buffer = read_hex_file("data_message.bin").unwrap();
        let data_frame = parse_data_frames(&data_buffer, &config_frame).unwrap();

        let base_timestamp = data_frame.prefix.soc as u64 * 1000;

        // Insert 10 frames with incrementing timestamps
        for i in 0..10 {
            let mut frame_data = HashMap::new();
            let timestamp = base_timestamp + (i * 33) as u64; // ~30fps

            if let PMUFrameType::Fixed(pmu_data) = &data_frame.data[0] {
                let phasor_values = pmu_data.parse_phasors(pmu_config);
                for (j, values) in phasor_values.iter().enumerate() {
                    if let PMUValues::Fixed(v) = values {
                        frame_data
                            .insert(column_names[j].clone(), PMUValue::FixedPhasor([v[0], v[1]]));
                    }
                }

                if let PMUValues::Float(analog_values) = pmu_data.parse_analogs(pmu_config) {
                    for (j, &value) in analog_values.iter().enumerate() {
                        frame_data.insert(
                            column_names[pmu_config.phnmr as usize + j].clone(),
                            PMUValue::Analog(value),
                        );
                    }
                }

                let digital_values = pmu_data.parse_digitals();
                for (j, &value) in digital_values.iter().enumerate() {
                    frame_data.insert(
                        column_names[pmu_config.phnmr as usize + pmu_config.annmr as usize + j]
                            .clone(),
                        PMUValue::Digital(value),
                    );
                }
            }

            data_store.add_frame(timestamp, frame_data);
        }

        // Test retrieval
        let slice = data_store.get_last_n_seconds(1).unwrap();
        assert!(slice.count > 0);

        // Verify data for each column
        for name in column_names {
            let column_data = data_store.get_column_slice(&name, &slice);
            assert!(column_data.is_some());
        }
    }

    #[test]
    fn test_buffer_wraparound() {
        let mut store = PMUDataStore::new(1, 5); // Small buffer to test wraparound
        store.add_column("test".to_string(), ColumnType::Phasor);

        // Add frames with increasing timestamps
        for i in 0..10 {
            let mut frame_data = HashMap::new();
            frame_data.insert("test".to_string(), PMUValue::Phasor([i as f32, 0.0]));
            store.add_frame((i + 1) * 1000, frame_data); // Start from 1000 to avoid 0
        }

        let slice = store.get_last_n_seconds(5).unwrap();
        assert!(slice.wraps);
        assert!(slice.count <= 5);

        // Verify the data
        if let Some(values) = store.get_column_slice("test", &slice) {
            assert!(!values.is_empty());
            assert!(values.len() <= 5);

            // Check that values are in sequence
            let mut last_value = 0.0;
            for value in values {
                match value {
                    PMUValue::Phasor([v, _]) => {
                        assert!(v > last_value);
                        last_value = v;
                    }
                    _ => panic!("Wrong type"),
                }
            }
        } else {
            panic!("Failed to get column slice");
        }
    }

    // Add a test for empty buffer
    #[test]
    fn test_empty_buffer() {
        let store = PMUDataStore::new(1, 5);
        assert!(store.get_last_n_seconds(1).is_none());
    }

    // Add a test for single frame
    #[test]
    fn test_single_frame() {
        let mut store = PMUDataStore::new(1, 5);
        store.add_column("test".to_string(), ColumnType::Phasor);

        let mut frame_data = HashMap::new();
        frame_data.insert("test".to_string(), PMUValue::Phasor([1.0, 0.0]));
        store.add_frame(1000, frame_data);

        let slice = store.get_last_n_seconds(1).unwrap();
        assert_eq!(
            slice.count, 1,
            "Expected count to be 1, got {}",
            slice.count
        );
        assert!(!slice.wraps);

        if let Some(values) = store.get_column_slice("test", &slice) {
            assert_eq!(values.len(), 1, "Expected one value, got {}", values.len());
            match &values[0] {
                PMUValue::Phasor([v, _]) => assert_eq!(*v, 1.0),
                _ => panic!("Wrong type"),
            }
        } else {
            panic!("Failed to get column slice");
        }
    }

    #[test]
    fn test_timestamp_retrieval() {
        let mut store = PMUDataStore::new(1, 100);
        store.add_column("test".to_string(), ColumnType::Analog);

        // Insert values with specific timestamps
        let timestamps = vec![1000, 2000, 3000, 4000, 5000];
        for &ts in &timestamps {
            let mut frame_data = HashMap::new();
            frame_data.insert("test".to_string(), PMUValue::Analog(ts as f32));
            store.add_frame(ts, frame_data);
        }

        // Test different time ranges
        let slice = store.get_last_n_seconds(2).unwrap();
        assert!(slice.count >= 2);

        //let empty_slice = store.get_last_n_seconds(0);
        //assert!(empty_slice.is_none());
    }
}
