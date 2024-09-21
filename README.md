# MinIO and Delta Lake Test Project

This project demonstrates the use of MinIO as an S3-compatible object store and Delta Lake for data storage and querying. It includes functionality to create files in MinIO, write Delta tables, and query Delta tables using DuckDB.

## Prerequisites

- Python 3.7+
- MinIO server running locally (or accessible endpoint)
- Required Python packages (see `requirements.txt`)

## Installation

1. Clone this repository:
   ```
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

3. Ensure your MinIO server is running and accessible.

## Usage

The project provides a CLI interface for various operations:

### Create a file in MinIO

```
python test.py minio create_file --file_name=example.txt
```

### List contents of MinIO bucket

```
python test.py minio list_contents
```

### Write a Delta table

```
python test.py delta write_delta_table --num_rows=20 --table_name=my_delta_table
```

### Query a Delta table

```
python test.py query_table --table_name=my_delta_table --query="SELECT * FROM delta_view LIMIT 10"
```

If no query is provided, it will default to showing the first 5 rows.

## Configuration

MinIO and Delta Lake configurations are set in the `MinioTest` and `DeltaLakeTest` classes respectively. Modify these as needed to match your environment.

## Troubleshooting

If you encounter issues with S3/MinIO connectivity, the script will attempt to write and read Delta tables from the local filesystem instead.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

[Specify your license here]