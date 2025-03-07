# Vector Database Project

This project implements a vector database using DuckDB. It provides a simple interface for connecting to the database, executing queries, and managing data.

## Project Structure

```
vector-database-project
├── src
│   ├── main.py                # Entry point of the application
│   ├── database
│   │   └── duckdb_connector.py # Contains the DuckDBConnector class
│   └── utils
│       └── helpers.py         # Utility functions for data handling
├── requirements.txt           # Project dependencies
├── .gitignore                 # Files and directories to ignore by Git
└── README.md                  # Project documentation
```

## Setup Instructions

1. Clone the repository:
   ```
   git clone <repository-url>
   cd vector-database-project
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

To run the application, execute the following command:
```
python src/main.py
```

## Features

- Connect to a DuckDB database
- Execute SQL queries
- Load and preprocess data for insertion into the database

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License

This project is licensed under the MIT License.