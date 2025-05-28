# Real Time Phasor Analytics

> [!CAUTION]
> This software is experimental and subject to change.

## Overview
This project is a Rust library for real-time phasor analytics. It provides a python interface for processing and accumulating IEEE C37.118 synchrophasor data into an in-memory arrow table. The core libraries found in the Rust code can also be used independently for other downstream applications.

## Notable features

- **Efficient Data Processing**: RTPA is designed to handle large volumes of synchrophasor data efficiently, ensuring minimal latency and high throughput.
- **Real-time Analysis**: RTPA can perform real-time analysis on incoming data streams, enabling immediate response to changes in the power grid.
- **Automatic Phasor Data Conversion**: RTPA optionally converts incoming synchrophasor data into Polar or Rectangular format if desired.


## Getting started

Clone the entire repo

```console
git clone https://github.com/G-PST/pmu-data-analytics.git
```

Running the CLI using
[cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html):

```console
cargo run --help
```

## Building the Python Module

It is recommended to create a virtual environment to install dependencies for building the RTPA python module.

```bash
python3 -m venv venv
source venv/bin/activate
```

- On **Windows**:
```bash
.venv\Scripts\activate
```

- On **Linux** or **macOS**:
```bash
source .venv/bin/activate
```

Next install the dependencies required to compile the Rust code into a python module.

```bash
pip install maturin patchelf
```

Navigate to the **py** directory and build+install the python wheel.

```bash
cd py
```

Build and install the wheel into your activated virtual environment.

```bash
maturin develop
```

To build a release wheel. (This will create a wheel file in the **RTPA-core/target/wheels** directory)

```bash
maturin build --release
```

To test, build and start the mock pdc using the following command.

```bash
cargo run mock-pdc
```

Then test the rtpa module, which points to the running mock pdc server above. It will wait and fill the buffer with data before returning a dataframe result.
```bash
python test_openpdc.py --host 127.0.0.1 --port 8123
```

## Running the PDC Server for Testing

To facilitate testing and development of synchrophasor data processing, you can run either a mock PDC server using a Rust-based tool or an `openPDC` container using Docker. Below are simple instructions for setting up and running these services.

### Option 1: Running the Mock PDC Server with Cargo

If you prefer a lightweight mock server for testing purposes, you can use the mock PDC provided by the Rust package. This option does not require Docker and runs directly on your system.

1. Ensure you have Rust and Cargo installed. If not, follow the installation instructions at [https://www.rust-lang.org/tools/install](https://www.rust-lang.org/tools/install).
2. Open a terminal or command prompt.
3. Run the following command to start the mock PDC server:

Run the mock-pdc with variable number of PMUs. (Max of ~130 PMUs)
If --num-pmus not given, the mock-pdc will send repeated copies of the IEEE example data frame, with updated timestamps.

```bash
cargo run mock-pdc --num-pmus=100
```

This will start the mock PDC server on `127.0.0.1:8123`. If you need to run it in the background, you can append `&` on Linux/macOS or use a tool like `start` on Windows.

To stop the server, simply press `Ctrl+C` in the terminal where it's running, or if it's in the background, find and kill the process.

### Option 2: Running the openPDC Container with Docker

The `openPDC` container provides a fully functional Phasor Data Concentrator (PDC) server. This option uses Docker to run the containerized application.

1. Open a terminal or command prompt.
2. Run the following command to pull and start the `openPDC` container in detached mode (this runs it in the background and prevents terminal spam):

```bash
docker run -d --name openPDC -p 8280:8280 -p 8900:8900 gridprotectionalliance/openpdc:v2.9.148
```

This will download the image (if not already present) and start the container, making it accessible on ports `8280` and `8900` on your local machine.
Visit the dockerhub [page](https://hub.docker.com/r/gridprotectionalliance/openpdc) for more information about the latest releases.

To stop and remove the container, run:

```bash
docker stop openPDC && docker rm openPDC
```

#### Installing Docker

If you don't have Docker installed on your system, follow the appropriate link below for installation instructions:

- **Windows**: [Install Docker Desktop on Windows](https://docs.docker.com/desktop/install/windows-install/)
- **macOS**: [Install Docker Desktop on Mac](https://docs.docker.com/desktop/install/mac-install/)
- **Linux**: [Install Docker Engine on Linux](https://docs.docker.com/engine/install/)

Docker Desktop (for Windows and macOS) or Docker Engine (for Linux) will provide the necessary tools to run containers on your machine.

### Running the Python Script

Once either the mock PDC server or the `openPDC` container is running, you can execute the Python script to connect and process data:

1. Ensure your Python environment is set up with the required dependencies (e.g., the `rtpa` package. See **Building The Python Module** above).
2. Run the script with:

```bash
python test_openpdc.py
```

If you're connecting to the `openPDC` container instead of the mock server, modify the connection details in `test_openpdc.py` to point to `127.0.0.1:8900` (as shown in the commented-out line in the script).


## Additional details

### Login to OpenPDC

You can log in to the local [openPDC](http://127.0.0.1:8280) instance and update the configuration. The default username and password are.

**USER**
```
.\admin
```

**PASSWORD**
```
admin
```

You can also install the latest release of openPDC with more features [here](https://github.com/GridProtectionAlliance/openPDC/releases/tag/v2.9.148)


### Using the PMU Connection Tester

Another option for Windows users is to install the PMU Connection Tester found [here](https://github.com/GridProtectionAlliance/PMUConnectionTester/releases).
Users will need to read the documentation for the PMU Connection Tester to understand how to use it effectively.
