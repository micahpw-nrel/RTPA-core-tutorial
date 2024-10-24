use bytes::BytesMut;
use chrono::{DateTime, TimeZone, Utc};
use parking_lot::RwLock;
use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use std::sync::Arc;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::frame_buffer::{ColumnType, PMUDataStore, PMUValue};
use crate::frame_parser::{parse_config_frame_1and2, parse_data_frames, Frame};
use crate::frames::{CommandFrame2011, ConfigurationFrame1and2_2011};
use std::collections::HashMap;

#[pyclass]
pub struct PMUClient {
    stream: Option<TcpStream>,
    config: Option<Arc<ConfigurationFrame1and2_2011>>,
    data_store: Arc<RwLock<PMUDataStore>>,
    read_task: Option<JoinHandle<()>>,
    stop_tx: Option<mpsc::Sender<()>>,
}

#[pymethods]
impl PMUClient {
    #[new]
    fn new(capacity: usize) -> Self {
        PMUClient {
            stream: None,
            config: None,
            data_store: Arc::new(RwLock::new(PMUDataStore::new(0, capacity))),
            read_task: None,
            stop_tx: None,
        }
    }

    fn connect(&mut self, hostname: &str, port: u16) -> PyResult<()> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let stream =
            rt.block_on(async { TcpStream::connect(format!("{}:{}", hostname, port)).await })?;
        self.stream = Some(stream);
        Ok(())
    }

    fn request_config(&mut self) -> PyResult<()> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            if let Some(stream) = &mut self.stream {
                // Create and send config request command
                let cmd = CommandFrame2011::new_send_config_frame1(1);
                let cmd_bytes = cmd.to_hex();
                stream.write_all(&cmd_bytes).await?;

                // Read response
                let mut buffer = BytesMut::with_capacity(2048);
                let bytes_read = stream.read_buf(&mut buffer).await?;

                if bytes_read > 0 {
                    if let Ok(Frame::Configuration(config)) = parse_config_frame_1and2(&buffer) {
                        // Initialize data store with configuration
                        let mut data_store = self.data_store.write();
                        for pmu_config in &config.pmu_configs {
                            for name in pmu_config.get_column_names() {
                                // Add appropriate column types based on config
                                let column_type = if pmu_config.format & 0x0002 != 0 {
                                    ColumnType::Phasor
                                } else {
                                    ColumnType::FixedPhasor
                                };
                                data_store.add_column(name, column_type);
                            }
                        }
                        self.config = Some(Arc::new(config));
                    }
                }
            }
            Ok::<(), io::Error>(())
        })?;
        Ok(())
    }

    fn start_streaming(&mut self) -> PyResult<()> {
        if self.config.is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Configuration not received. Call request_config first.",
            ));
        }

        let (stop_tx, mut stop_rx) = mpsc::channel(1);
        self.stop_tx = Some(stop_tx);

        let stream = self.stream.take().unwrap();
        let config = self.config.clone().unwrap();
        let data_store = self.data_store.clone();

        let handle = tokio::spawn(async move {
            let mut buffer = BytesMut::with_capacity(2048);
            let mut stream = stream;

            loop {
                tokio::select! {
                    _ = stop_rx.recv() => break,
                    result = stream.read_buf(&mut buffer) => {
                        match result {
                            Ok(0) => break, // EOF
                            Ok(n) => {
                                if let Ok(Frame::Data(data_frame)) = parse_data_frames(&buffer[..n], &config) {
                                    let mut frame_data = HashMap::new();

                                    // Convert data frame to frame_data HashMap
                                    for (pmu_idx, pmu_data) in data_frame.data.iter().enumerate() {
                                        let pmu_config = &config.pmu_configs[pmu_idx];
                                        let column_names = pmu_config.get_column_names();

                                        match pmu_data {
                                            PMUFrameType::Fixed(data) => {
                                                // Handle phasors
                                                let phasor_values = data.parse_phasors(pmu_config);
                                                for (i, values) in phasor_values.iter().enumerate() {
                                                    let name = &column_names[i];
                                                    match values {
                                                        crate::frames::PMUValues::Fixed(v) => {
                                                            frame_data.insert(name.clone(),
                                                                PMUValue::FixedPhasor([v[0], v[1]]));
                                                        },
                                                        _ => {}
                                                    }
                                                }

                                                // Handle analog values
                                                let analog_values = data.parse_analogs(pmu_config);
                                                match analog_values {
                                                    crate::frames::PMUValues::Fixed(v) => {
                                                        for (i, &value) in v.iter().enumerate() {
                                                            let name = &column_names[pmu_config.phnmr as usize + i];
                                                            frame_data.insert(name.clone(),
                                                                PMUValue::FixedAnalog(value));
                                                        }
                                                    },
                                                    _ => {}
                                                }

                                                // Handle digital values
                                                let digital_values = data.parse_digitals();
                                                for (i, &value) in digital_values.iter().enumerate() {
                                                    let name = &column_names[
                                                        (pmu_config.phnmr + pmu_config.annmr) as usize + i];
                                                    frame_data.insert(name.clone(),
                                                        PMUValue::Digital(value));
                                                }
                                            },
                                            PMUFrameType::Floating(_) => {
                                                // Handle floating point data similarly...
                                            }
                                        }
                                    }

                                    let timestamp = data_frame.prefix.soc as u64 * 1000 +
                                                  (data_frame.prefix.fracsec & 0x00FFFFFF) as u64;
                                    data_store.write().add_frame(timestamp, frame_data);
                                }
                                buffer.clear();
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });

        self.read_task = Some(handle);
        Ok(())
    }

    fn stop_streaming(&mut self) -> PyResult<()> {
        if let Some(tx) = self.stop_tx.take() {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let _ = tx.send(()).await;
            });
        }
        if let Some(handle) = self.read_task.take() {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let _ = handle.await;
            });
        }
        Ok(())
    }
    fn get_last_n_seconds_df(&self, seconds: u64) -> PyResult<PyDataFrame> {
        let data_store = self.data_store.read();
        let config = self.config.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Configuration not available")
        })?;

        if let Some(slice) = data_store.get_last_n_seconds(seconds) {
            // Get timestamps
            let timestamps = data_store.get_timestamps_slice(&slice);

            // Convert timestamps to DateTime
            let datetime_values: Vec<DateTime<Utc>> = timestamps
                .iter()
                .map(|&ts| {
                    let secs = (ts / 1000) as i64;
                    let nsecs = ((ts % 1000) * 1_000_000) as u32;
                    Utc.timestamp_opt(secs, nsecs).unwrap()
                })
                .collect();

            // Create timestamp series
            let mut df = DataFrame::new(vec![Series::new("timestamp", datetime_values)])?;

            // Add data columns for each PMU
            for pmu_config in &config.pmu_configs {
                let column_names = pmu_config.get_column_names();

                for name in column_names {
                    if let Some(values) = data_store.get_column_slice(&name, &slice) {
                        // Convert PMUValues to appropriate Series based on type
                        match &values[0] {
                            PMUValue::Phasor([_, _]) => {
                                let mag: Vec<f32> = values
                                    .iter()
                                    .map(|v| match v {
                                        PMUValue::Phasor([m, _]) => *m,
                                        _ => unreachable!(),
                                    })
                                    .collect();
                                let ang: Vec<f32> = values
                                    .iter()
                                    .map(|v| match v {
                                        PMUValue::Phasor([_, a]) => *a,
                                        _ => unreachable!(),
                                    })
                                    .collect();
                                df.with_column(Series::new(&format!("{}_magnitude", name), mag))?;
                                df.with_column(Series::new(&format!("{}_angle", name), ang))?;
                            }
                            PMUValue::FixedPhasor([_, _]) => {
                                let mag: Vec<i16> = values
                                    .iter()
                                    .map(|v| match v {
                                        PMUValue::FixedPhasor([m, _]) => *m,
                                        _ => unreachable!(),
                                    })
                                    .collect();
                                let ang: Vec<i16> = values
                                    .iter()
                                    .map(|v| match v {
                                        PMUValue::FixedPhasor([_, a]) => *a,
                                        _ => unreachable!(),
                                    })
                                    .collect();
                                df.with_column(Series::new(&format!("{}_magnitude", name), mag))?;
                                df.with_column(Series::new(&format!("{}_angle", name), ang))?;
                            }
                            PMUValue::Analog(v) => {
                                let float_values: Vec<f32> = values
                                    .iter()
                                    .map(|v| match v {
                                        PMUValue::Analog(val) => *val,
                                        _ => unreachable!(),
                                    })
                                    .collect();
                                df.with_column(Series::new(&name, float_values))?;
                            }
                            PMUValue::FixedAnalog(v) => {
                                let int_values: Vec<i16> = values
                                    .iter()
                                    .map(|v| match v {
                                        PMUValue::FixedAnalog(val) => *val,
                                        _ => unreachable!(),
                                    })
                                    .collect();
                                df.with_column(Series::new(&name, int_values))?;
                            }
                            PMUValue::Digital(v) => {
                                let digital_values: Vec<u16> = values
                                    .iter()
                                    .map(|v| match v {
                                        PMUValue::Digital(val) => *val,
                                        _ => unreachable!(),
                                    })
                                    .collect();
                                df.with_column(Series::new(&name, digital_values))?;
                            }
                        }
                    }
                }
            }

            Ok(PyDataFrame(df))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data available",
            ))
        }
    }
    fn get_last_n_seconds(&self, seconds: u64) -> PyResult<Vec<HashMap<String, PyObject>>> {
        Python::with_gil(|py| {
            let data_store = self.data_store.read();
            if let Some(slice) = data_store.get_last_n_seconds(seconds) {
                let mut result = Vec::new();
                // Get timestamps
                let timestamps = data_store.get_timestamps_slice(&slice);

                // Get column names from config
                if let Some(config) = &self.config {
                    for pmu_config in &config.pmu_configs {
                        let column_names = pmu_config.get_column_names();
                        for name in column_names {
                            if let Some(values) = data_store.get_column_slice(&name, &slice) {
                                let mut frame_data = HashMap::new();
                                frame_data
                                    .insert("timestamp".to_string(), timestamps[0].into_py(py));
                                frame_data.insert("name".to_string(), name.into_py(py));
                                match &values[0] {
                                    PMUValue::Phasor(v) => {
                                        frame_data
                                            .insert("value".to_string(), v.to_vec().into_py(py));
                                    }
                                    PMUValue::FixedPhasor(v) => {
                                        frame_data
                                            .insert("value".to_string(), v.to_vec().into_py(py));
                                    }
                                    PMUValue::Analog(v) => {
                                        frame_data.insert("value".to_string(), v.into_py(py));
                                    }
                                    PMUValue::FixedAnalog(v) => {
                                        frame_data.insert("value".to_string(), v.into_py(py));
                                    }
                                    PMUValue::Digital(v) => {
                                        frame_data.insert("value".to_string(), v.into_py(py));
                                    }
                                }
                                result.push(frame_data);
                            }
                        }
                    }
                }
                Ok(result)
            } else {
                Ok(Vec::new())
            }
        })
    }
}

impl Drop for PMUClient {
    fn drop(&mut self) {
        let _ = self.stop_streaming();
    }
}

#[pymodule]
fn pmu_client(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PMUClient>()?;
    Ok(())
}
