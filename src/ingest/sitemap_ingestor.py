import requests
from bs4 import BeautifulSoup
from sentence_transformers import SentenceTransformer
from database.duckdb_connector import DuckDBConnector
import numpy as np

class SitemapIngestor:
    def __init__(self, sitemap_url, db_connector):
        self.sitemap_url = sitemap_url
        self.db_connector = db_connector
        self.model = SentenceTransformer('all-MiniLM-L6-v2')

    def fetch_sitemap(self):
        response = requests.get(self.sitemap_url)
        response.raise_for_status()
        return response.text

    def parse_sitemap(self, sitemap_xml):
        soup = BeautifulSoup(sitemap_xml, 'xml')
        urls = [loc.text for loc in soup.find_all('loc')]
        return urls

    def embed_text(self, text):
        """
        Generates embeddings as a fixed-length FLOAT[384] array.
        """
        embedding = self.model.encode(text)  # Returns NumPy array
        embedding = np.array(embedding, dtype=np.float32)  # Ensure FLOAT32
        print("\n Generated Embedding:", embedding[:10]) 
        if embedding.shape[0] != 384:
            raise ValueError(f"Embedding size mismatch! Expected 384, got {embedding.shape[0]}")
        return embedding.tolist() 

    def extract_embeddings(self):
        """
        Processes sitemap URLs and yields (url, embedding) pairs for insertion.
        """
        sitemap_xml = self.fetch_sitemap()
        urls = self.parse_sitemap(sitemap_xml)

        for url in urls:
            response = requests.get(url)
            response.raise_for_status()
            text = response.text
            embedding = self.embed_text(text)
            yield url, embedding 

    def ingest(self):
        sitemap_xml = self.fetch_sitemap()
        urls = self.parse_sitemap(sitemap_xml)

        for url in urls:
            response = requests.get(url)
            response.raise_for_status()
            text = response.text
            embedding = self.embed_text(text)
            
            print(f"INSERTING: {url}")  # Debugging log
            self.db_connector.execute_query(
                "INSERT INTO embeddings (url, embedding) VALUES (?, ?)",
                (url, embedding)
            )
        
        print("Ingestion complete. Verifying...")
        count = self.db_connector.execute_query("SELECT COUNT(*) FROM embeddings;")
        print(f"Total records in DB: {count[0][0]}")



