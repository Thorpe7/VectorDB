import duckdb

class DuckDBConnector:
    def __init__(self, database_path):
        self.database_path = database_path
        self.connection = None

    def connect(self):
        self.connection = duckdb.connect(self.database_path)

        # Ensure VSS extension and indexing is enabled
        self.connection.execute("INSTALL vss;")
        self.connection.execute("LOAD vss;")
        self.connection.execute("SET GLOBAL hnsw_enable_experimental_persistence = true;")

        # Drop and recreate the table with fixed-size FLOAT[384]
        self.connection.execute("DROP TABLE IF EXISTS embeddings;")
        self.connection.execute("""
            CREATE TABLE embeddings (
                url TEXT,
                embedding FLOAT[384]
            )
        """)

        # Create the HNSW index
        self.connection.execute("""
            CREATE INDEX hnsw_idx ON embeddings USING HNSW (embedding)
            WITH (metric = 'cosine'); 
        """)

    def insert_embedding(self, url: str, embedding: list):
        """
        Inserts an embedding into DuckDB, ensuring correct format.
        """
        if self.connection is None:
            raise Exception("Connection not established. Call connect() first.")

        if len(embedding) != 384:
            raise ValueError(f"Embedding size mismatch! Expected 384, got {len(embedding)}")

        query = "INSERT INTO embeddings (url, embedding) VALUES (?, ?)"
        self.connection.execute(query, (url, embedding)) 

    def execute_query(self, query, params=None):
        if self.connection is None:
            raise Exception("Connection not established. Call connect() first.")
        if params:
            return self.connection.execute(query, params).fetchall()
        return self.connection.execute(query).fetchall()

    def close_connection(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

if __name__ == "__main__": 

    # Connect to DuckDB
    conn = duckdb.connect("src/database/vector.db")

    # Print some values from the table
    rows = conn.execute("SELECT url, embedding FROM embeddings LIMIT 5;").fetchall()

    # Print results
    for row in rows:
        print(f"URL: {row[0]}")
        print(f"Embedding (first 5 values): {row[1][:5]}")  # Print only first 5 values
        print("=" * 50)

