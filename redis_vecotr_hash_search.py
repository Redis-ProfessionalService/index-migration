import redis
import numpy as np
from typing import Dict, List, Any

# Connect to Redis Enterprise
# Replace with your Redis host, port, and credentials
client = redis.Redis(
    host='node1.cluster-kmiller.ps-redis.com',
    port=17120,
    decode_responses=True  # Automatically decode responses to strings
)

# Configuration for multiple indexes
INDEX_CONFIGS = {
    'docIdx': {
        'prefix': 'vector:doc:',
        'embedding_size': 4,
        'schema': [
            'embedding', 'VECTOR', 'FLAT', '6', 'DIM', '4', 'TYPE', 'FLOAT32', 'DISTANCE_METRIC', 'COSINE',
            'embedding_model', 'TEXT',
            'chunk_strategy', 'TEXT',
            'publication_time', 'NUMERIC', 'SORTABLE',
            'source_id', 'TEXT',
            'document_id', 'TEXT',
            'chunk_number', 'TEXT'
        ]
    },
    'imageIdx': {
        'prefix': 'vector:img:',
        'embedding_size': 3,
        'schema': [
            'embedding', 'VECTOR', 'FLAT', '6', 'DIM', '3', 'TYPE', 'FLOAT32', 'DISTANCE_METRIC', 'COSINE',
            'image_type', 'TEXT',
            'resolution', 'TEXT',
            'upload_time', 'NUMERIC', 'SORTABLE',
            'image_id', 'TEXT',
            'category', 'TEXT'
        ]
    },
    'audioIdx': {
        'prefix': 'vector:audio:',
        'embedding_size': 5,
        'schema': [
            'embedding', 'VECTOR', 'FLAT', '6', 'DIM', '5', 'TYPE', 'FLOAT32', 'DISTANCE_METRIC', 'COSINE',
            'audio_format', 'TEXT',
            'duration', 'NUMERIC',
            'recording_time', 'NUMERIC', 'SORTABLE',
            'audio_id', 'TEXT',
            'genre', 'TEXT'
        ]
    }
}

def create_index(redis_index_name: str, config: Dict[str, Any]) -> None:
    """Create an index with the specified schema including a vector field."""
    try:
        # Create index with the specified schema
        command = ['FT.CREATE', redis_index_name, 'ON', 'HASH', 'PREFIX', '1', config['prefix'], 'SCHEMA']
        command.extend(config['schema'])
        
        client.execute_command(*command)
        print(f"Index '{redis_index_name}' created successfully with vector field.")
    except redis.RedisError as e:
        print(f"Error creating index: {e}")

def add_sample_data(config: Dict[str, Any]) -> None:
    """Add sample Hash data with vectors to Redis."""
    try:
        # Sample data based on index type
        if config['prefix'] == 'vector:doc:':
            items = [
                {
                    'key': f'{config["prefix"]}1',
                    'value': {
                        'embedding': np.array([0.1, 0.2, 0.3, 0.4][:config['embedding_size']], dtype=np.float32).tobytes(),
                        'embedding_model': 'bert-base',
                        'chunk_strategy': 'sentence',
                        'publication_time': 1625097600,
                        'source_id': 'src_001',
                        'document_id': 'doc_001',
                        'chunk_number': '1'
                    }
                },
                {
                    'key': f'{config["prefix"]}2',
                    'value': {
                        'embedding': np.array([0.2, 0.1, 0.4, 0.3][:config['embedding_size']], dtype=np.float32).tobytes(),
                        'embedding_model': 'bert-large',
                        'chunk_strategy': 'paragraph',
                        'publication_time': 1625184000,
                        'source_id': 'src_002',
                        'document_id': 'doc_002',
                        'chunk_number': '2'
                    }
                },
                {
                    'key': f'{config["prefix"]}3',
                    'value': {
                        'embedding': np.array([0.3, 0.4, 0.1, 0.2][:config['embedding_size']], dtype=np.float32).tobytes(),
                        'embedding_model': 'roberta-base',
                        'chunk_strategy': 'sentence',
                        'publication_time': 1625270400,
                        'source_id': 'src_003',
                        'document_id': 'doc_003',
                        'chunk_number': '1'
                    }
                },
                {
                    'key': f'{config["prefix"]}4',
                    'value': {
                        'embedding': np.array([0.4, 0.3, 0.2, 0.1][:config['embedding_size']], dtype=np.float32).tobytes(),
                        'embedding_model': 'distilbert',
                        'chunk_strategy': 'paragraph',
                        'publication_time': 1625356800,
                        'source_id': 'src_004',
                        'document_id': 'doc_004',
                        'chunk_number': '1'
                    }
                },
                {
                    'key': f'{config["prefix"]}5',
                    'value': {
                        'embedding': np.array([0.5, 0.1, 0.2, 0.3][:config['embedding_size']], dtype=np.float32).tobytes(),
                        'embedding_model': 'bert-base',
                        'chunk_strategy': 'sentence',
                        'publication_time': 1625443200,
                        'source_id': 'src_005',
                        'document_id': 'doc_005',
                        'chunk_number': '2'
                    }
                }
            ]
        elif config['prefix'] == 'vector:img:':
            items = [
                {
                    'key': f'{config["prefix"]}1',
                    'value': {
                        'embedding': np.array([0.3, 0.4, 0.5][:config['embedding_size']], dtype=np.float32).tobytes(),
                        'image_type': 'jpg',
                        'resolution': '1920x1080',
                        'upload_time': 1625097600,
                        'image_id': 'img_001',
                        'category': 'landscape'
                    }
                },
                {
                    'key': f'{config["prefix"]}2',
                    'value': {
                        'embedding': np.array([0.4, 0.3, 0.6][:config['embedding_size']], dtype=np.float32).tobytes(),
                        'image_type': 'png',
                        'resolution': '2560x1440',
                        'upload_time': 1625184000,
                        'image_id': 'img_002',
                        'category': 'portrait'
                    }
                },
                {
                    'key': f'{config["prefix"]}3',
                    'value': {
                        'embedding': np.array([0.5, 0.6, 0.3][:config['embedding_size']], dtype=np.float32).tobytes(),
                        'image_type': 'webp',
                        'resolution': '3840x2160',
                        'upload_time': 1625270400,
                        'image_id': 'img_003',
                        'category': 'nature'
                    }
                },
                {
                    'key': f'{config["prefix"]}4',
                    'value': {
                        'embedding': np.array([0.6, 0.5, 0.4][:config['embedding_size']], dtype=np.float32).tobytes(),
                        'image_type': 'jpeg',
                        'resolution': '1280x720',
                        'upload_time': 1625356800,
                        'image_id': 'img_004',
                        'category': 'architecture'
                    }
                },
                {
                    'key': f'{config["prefix"]}5',
                    'value': {
                        'embedding': np.array([0.7, 0.4, 0.5][:config['embedding_size']], dtype=np.float32).tobytes(),
                        'image_type': 'png',
                        'resolution': '2048x1536',
                        'upload_time': 1625443200,
                        'image_id': 'img_005',
                        'category': 'abstract'
                    }
                }
            ]
        else:  # audioIdx
            items = [
                {
                    'key': f'{config["prefix"]}1',
                    'value': {
                        'embedding': np.array([0.5, 0.6, 0.7, 0.8, 0.9][:config['embedding_size']], dtype=np.float32).tobytes(),
                        'audio_format': 'mp3',
                        'duration': 180,
                        'recording_time': 1625097600,
                        'audio_id': 'audio_001',
                        'genre': 'classical'
                    }
                },
                {
                    'key': f'{config["prefix"]}2',
                    'value': {
                        'embedding': np.array([0.6, 0.5, 0.8, 0.7, 0.9][:config['embedding_size']], dtype=np.float32).tobytes(),
                        'audio_format': 'wav',
                        'duration': 240,
                        'recording_time': 1625184000,
                        'audio_id': 'audio_002',
                        'genre': 'jazz'
                    }
                },
                {
                    'key': f'{config["prefix"]}3',
                    'value': {
                        'embedding': np.array([0.7, 0.8, 0.5, 0.6, 0.9][:config['embedding_size']], dtype=np.float32).tobytes(),
                        'audio_format': 'flac',
                        'duration': 320,
                        'recording_time': 1625270400,
                        'audio_id': 'audio_003',
                        'genre': 'rock'
                    }
                },
                {
                    'key': f'{config["prefix"]}4',
                    'value': {
                        'embedding': np.array([0.8, 0.7, 0.6, 0.5, 0.9][:config['embedding_size']], dtype=np.float32).tobytes(),
                        'audio_format': 'aac',
                        'duration': 200,
                        'recording_time': 1625356800,
                        'audio_id': 'audio_004',
                        'genre': 'electronic'
                    }
                },
                {
                    'key': f'{config["prefix"]}5',
                    'value': {
                        'embedding': np.array([0.9, 0.8, 0.7, 0.6, 0.5][:config['embedding_size']], dtype=np.float32).tobytes(),
                        'audio_format': 'ogg',
                        'duration': 280,
                        'recording_time': 1625443200,
                        'audio_id': 'audio_005',
                        'genre': 'pop'
                    }
                }
            ]

        # Store each item as a Hash
        for item in items:
            # Store all fields except 'embedding' as strings
            client.hset(item['key'], mapping={
                k: v for k, v in item['value'].items() if k != 'embedding'
            })
            # Store embedding as a binary string
            client.hset(item['key'], 'embedding', item['value']['embedding'])
            print(f"Added {item['key']} to Redis.")
    except redis.RedisError as e:
        print(f"Error adding data: {e}")

def vector_search(redis_index_name: str, config: Dict[str, Any]) -> None:
    """Perform a vector similarity search."""
    try:
        # Generate a query vector based on the embedding size
        query_vector = [0.15] * config['embedding_size']
        # Convert vector to bytes
        query_vector_bytes = np.array(query_vector, dtype=np.float32).tobytes()

        # Get all fields from the schema except 'embedding'
        return_fields = []
        for i in range(0, len(config['schema']), 2):
            if config['schema'][i] != 'embedding':
                return_fields.append(config['schema'][i])
        return_fields.append('score')

        # Perform KNN search: Find top 2 nearest neighbors
        result = client.execute_command(
            'FT.SEARCH', redis_index_name,
            '*=>[KNN 2 @embedding $BLOB AS score]',
            'PARAMS', 2, 'BLOB', query_vector_bytes,
            'RETURN', len(return_fields), *return_fields,
            'SORTBY', 'score'  # Sort by similarity score (ascending = closer)
        )

        print(f"\nVector Search Results for {redis_index_name} (top 2 nearest neighbors):")
        count = result[0]  # Number of results
        for i in range(1, len(result), 2):
            key = result[i]
            fields = result[i + 1]
            print(f"Key: {key}")
            print(f"Fields: {dict(zip(fields[::2], fields[1::2]))}")
    except redis.RedisError as e:
        print(f"Error performing vector search: {e}")

def list_indexes() -> None:
    """List all indexes in the database."""
    try:
        indexes = client.execute_command('FT._LIST')
        print("\nAvailable Indexes:")
        for idx in indexes:
            # Get index info including prefix
            info = client.execute_command('FT.INFO', idx)
            # Convert the flat list to a dictionary for easier access
            info_dict = dict(zip(info[::2], info[1::2]))
            
            # Get the prefix from index_definition
            index_def = info_dict.get('index_definition', [])
            prefix = None
            if index_def:
                # Find the 'prefixes' entry in the definition
                for i in range(len(index_def)):
                    if index_def[i] == 'prefixes':
                        prefix = index_def[i + 1][0] if index_def[i + 1] else 'None'
                        break
            
            print(f"- {idx} (prefix: {prefix})")
    except redis.RedisError as e:
        print(f"Error listing indexes: {e}")

def main() -> None:
    """Main function to run the example."""
    # Clear any existing indexes (optional, for clean testing)
    for index_name in INDEX_CONFIGS:
        try:
            client.execute_command('FT.DROPINDEX', index_name)
            print(f"Dropped existing index '{index_name}' (if it existed).")
        except redis.RedisError:
            pass  # Ignore if index doesn't exist

    # Create and populate all indexes
    for index_name, config in INDEX_CONFIGS.items():
        create_index(index_name, config)
        add_sample_data(config)
        vector_search(index_name, config)

    list_indexes()

    # # Clean up (optional)
    # for index_name in INDEX_CONFIGS:
    #     try:
    #         client.execute_command('FT.DROPINDEX', index_name)
    #         print(f"Cleaned up: Dropped index '{index_name}'.")
    #     except redis.RedisError as e:
    #         print(f"Error dropping index: {e}")

if __name__ == '__main__':
    try:
        main()
    finally:
        # Close the Redis connection
        client.close()