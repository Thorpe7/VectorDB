from database.duckdb_connector import DuckDBConnector
from ingest.sitemap_ingestor import SitemapIngestor

def main():
    db_connector = DuckDBConnector('src/database/vector.db')
    db_connector.connect()
    
    sitemap_url = 'https://docs.flywheel.io/sitemap.xml'
    ingestor = SitemapIngestor(sitemap_url, db_connector)

    ingestor.ingest()

    db_connector.close_connection()
    
if __name__ == "__main__": 
    main()
