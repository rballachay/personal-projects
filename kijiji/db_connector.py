# db_connector.py
import sqlite3
from typing import List, Dict, Any, Tuple
import numpy as np

class ListingManager:
    def __init__(self, db_path: str = "kijiji_listings.db"):
        self.db_path = db_path
        self._create_listings_table()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _create_listings_table(self):
        with self._connect() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS listings (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                description TEXT,
                imageUrls TEXT,
                price REAL,
                location TEXT
            )
            """)

    def insert_listing(self, listing: Dict[str, Any]) -> None:
        with self._connect() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO listings 
                (id, title, url, description, imageUrls, price, location)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                int(listing["id"]),
                listing["title"],
                listing["url"],
                listing.get("description"),
                str(listing.get("imageUrls", "")),
                float(listing["price"]) if listing.get("price") else None,
                listing.get("location")
            ))

    def get_all_listings(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            cursor = conn.execute("SELECT * FROM listings WHERE description IS NOT NULL")
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        

class EmbeddingManager:
    def __init__(self, db_path: str = "kijiji_listings.db"):
        self.db_path = db_path
        self._create_embeddings_table()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _create_embeddings_table(self):
        """Create a table to store listing embeddings."""
        with self._connect() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS embeddings (
                listing_id INTEGER PRIMARY KEY,
                embedding BLOB,
                FOREIGN KEY(listing_id) REFERENCES listings(id)
            )
            """)

    def insert_embedding(self, listing_id: int, embedding: np.ndarray):
        """Insert a single embedding."""
        with self._connect() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO embeddings (listing_id, embedding)
                VALUES (?, ?)
            """, (listing_id, embedding.tobytes()))

    def insert_many(self, embeddings: List[Tuple[int, np.ndarray]]):
        """Insert multiple embeddings at once."""
        with self._connect() as conn:
            conn.executemany("""
                INSERT OR REPLACE INTO embeddings (listing_id, embedding)
                VALUES (?, ?)
            """, [(lid, emb.tobytes()) for lid, emb in embeddings])

    def get_embedding(self, listing_id: int) -> np.ndarray:
        """Retrieve a single embedding by listing_id."""
        with self._connect() as conn:
            cur = conn.execute("SELECT embedding FROM embeddings WHERE listing_id=?", (listing_id,))
            row = cur.fetchone()
            if row:
                return np.frombuffer(row[0], dtype=np.float32)
            return None

    def get_all_embeddings(self) -> List[Tuple[int, np.ndarray]]:
        """Return all embeddings with their listing_id."""
        with self._connect() as conn:
            cursor = conn.execute("SELECT listing_id, embedding FROM embeddings")
            result = [(row[0], np.frombuffer(row[1], dtype=np.float32)) for row in cursor.fetchall()]
        return result
