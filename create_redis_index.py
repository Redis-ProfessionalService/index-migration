import redis
import numpy as np
from typing import List, Dict, Any

def create_redis_index(
    redis_host: str = "localhost",
    redis_port: int = 6379,
    redis_password: str = None,
    redis_index_name: str = "vector_index",
    redis_prefix: str = "doc:",
    embedding_size: int = 1536
) -> None:
    """
    Create a Redis index with the specified schema.
    
    Args:
        redis_host: Redis host address
        redis_port: Redis port number
        redis_password: Redis password (if any)
        redis_index_name: Name of the Redis index
        redis_prefix: Prefix for the Redis keys
        embedding_size: Size of the embedding vectors
    """
    # Connect to Redis
    r = redis.Redis(
        host=redis_host,
        port=redis_port,
        password=redis_password,
        decode_responses=True
    )
    
    # Create the index
    try:
        r.execute_command(
            'FT.CREATE', f'{redis_index_name}',
            'on', 'HASH',
            'PREFIX', '1', redis_prefix,
            'SCHEMA',
            'embedding', 'VECTOR', 'FLAT', '6', 'DIM', embedding_size, 'TYPE', 'FLOAT32', 'DISTANCE_METRIC', 'COSINE',
            'embedding_model', 'TEXT',
            'chunk_strategy', 'TEXT',
            'publication_time', 'NUMERIC', 'SORTABLE',
            'source_id', 'TEXT',
            'document_id', 'TEXT',
            'chunk_number', 'TEXT'
        )
        print(f"Successfully created Redis index: {redis_index_name}")
    except redis.exceptions.ResponseError as e:
        if "Index already exists" in str(e):
            print(f"Index {redis_index_name} already exists")
        else:
            raise e

def add_document(
    redis_client: redis.Redis,
    key: str,
    embedding: List[float],
    embedding_model: str,
    chunk_strategy: str,
    publication_time: int,
    source_id: str,
    document_id: str,
    chunk_number: str
) -> None:
    """
    Add a document to the Redis database.
    
    Args:
        redis_client: Redis client instance
        key: Redis key for the document
        embedding: Vector embedding
        embedding_model: Name of the embedding model used
        chunk_strategy: Strategy used for chunking
        publication_time: Publication timestamp
        source_id: Source identifier
        document_id: Document identifier
        chunk_number: Chunk number
    """
    # Convert embedding to bytes
    embedding_bytes = np.array(embedding, dtype=np.float32).tobytes()
    
    # Create document hash
    doc = {
        'embedding': embedding_bytes,
        'embedding_model': embedding_model,
        'chunk_strategy': chunk_strategy,
        'publication_time': publication_time,
        'source_id': source_id,
        'document_id': document_id,
        'chunk_number': chunk_number
    }
    
    # Add document to Redis
    redis_client.hset(key, mapping=doc)

if __name__ == "__main__":
    # Example usage
    redis_host = "localhost"
    redis_port = 6379
    redis_password = None
    redis_index_name = "vector_index"
    redis_prefix = "doc:"
    embedding_size = 1536
    
    # Create the index
    create_redis_index(
        redis_host=redis_host,
        redis_port=redis_port,
        redis_password=redis_password,
        redis_index_name=redis_index_name,
        redis_prefix=redis_prefix,
        embedding_size=embedding_size
    )
    
    # Example of adding a document
    r = redis.Redis(
        host=redis_host,
        port=redis_port,
        password=redis_password,
        decode_responses=True
    )
    
    # Example document
    example_doc = {
        'key': 'doc:1',
        'embedding': [0.1] * embedding_size,  # Example embedding
        'embedding_model': 'example-model',
        'chunk_strategy': 'fixed-size',
        'publication_time': 1234567890,
        'source_id': 'source1',
        'document_id': 'doc1',
        'chunk_number': '1'
    }
    
    add_document(
        redis_client=r,
        **example_doc
    ) 