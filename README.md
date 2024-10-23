# fetch_datalake

fetch_datalake is a  solution designed to extract, process, and manage csv datasets from Azure Data Lake. This repository provides a set of Python scripts and utilities that make it easy to query and retrieve data from a data lake, offering flexible configurations.

## Getting Started

Clone the Repository:

    ```bash
    git clone https://github.com/khadayatbibek/fetch_datalake.git
    cd fetch_datalake
    ```

Install Dependencies: Use pip to install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```

Configure Your Environment: Set up the necessary environment variables for your cloud provider.

Fetch Data: Modify the config.json to specify your target data lake, file format, and other parameters. Then run the fetch script:

    ```bash
    python fetch_data.py
    ```
