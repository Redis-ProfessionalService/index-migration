from redis import Redis
from redisvl.schema import IndexSchema
from redisvl.index import SearchIndex
from redis.commands.search.field import TextField, NumericField, VectorField
from redisvl.query import VectorQuery

# Helper function to retrieve index information from Redis
def get_index_definition(client, index_name):
    try:
        return client.ft(index_name).info()
    except Exception as e:
        print(f"Error retrieving index {index_name}: {e}")
        return None

def cleanup_target_database(target_client, index_name, prefix):
    """Clean up target database by removing existing index and matching keys"""
    try:
        # Delete the index if it exists
        try:
            target_client.ft(index_name).dropindex()
            print(f"Deleted existing index {index_name} from target database")
        except Exception as e:
            print(f"No existing index {index_name} to delete: {e}")

        # Delete all keys matching the prefix
        pattern = f"{prefix}*"
        cursor = 0
        while True:
            cursor, keys = target_client.scan(cursor, match=pattern, count=100)
            if keys:
                target_client.delete(*keys)
                print(f"Deleted {len(keys)} keys matching pattern {pattern}")
            if cursor == 0:
                break
        print("Target database cleanup completed")
    except Exception as e:
        print(f"Error during target database cleanup: {e}")
        raise

def recreate_index(target_client, index_info, index_name):
    """Recreate the index in target database with the same schema as source"""
    fields = []
    for field in index_info["attributes"]:
        # Convert field list to dictionary
        field_dict = {field[i]: field[i + 1] for i in range(0, len(field), 2)}
        field_name = field_dict.get("attribute", "")
        field_type = field_dict.get("type", "")
        
        if field_type == "TEXT":
            fields.append({
                "name": field_name,
                "type": "text"
            })
        elif field_type == "NUMERIC":
            fields.append({
                "name": field_name,
                "type": "numeric"
            })
        elif field_type == "VECTOR":
            fields.append({
                "name": field_name,
                "type": "vector",
                "attrs": {
                    "algorithm": field_dict.get("algorithm", "FLAT"),
                    "dims": int(field_dict.get("dim", 3)),
                    "distance_metric": field_dict.get("distance_metric", "COSINE"),
                    "type": field_dict.get("data_type", "FLOAT32")
                }
            })

    # Extract prefix from index definition
    index_def = index_info["index_definition"]
    prefix = None
    for i in range(0, len(index_def), 2):
        if index_def[i] == "prefixes" and isinstance(index_def[i + 1], list) and len(index_def[i + 1]) > 0:
            prefix = index_def[i + 1][0]  # Take the first prefix
            break
    
    if not prefix:
        raise Exception("No prefix found in index definition")

    # Create schema dictionary for the new index
    schema_dict = {
        "index": {
            "name": index_name,
            "prefix": prefix
        },
        "fields": fields
    }

    # Create and initialize the new index
    schema = IndexSchema.from_dict(schema_dict)
    index = SearchIndex(schema, target_client)
    index.create(overwrite=True)
    print(f"Index {index_name} created in target database")
    
    return prefix  # Return the prefix for use in replication

def run_riot_replication(source_client, target_client, key_pattern):
    """Execute RIOT replication to migrate data from source to target database"""
    import subprocess
    
    source_url = f"redis://{source_client.connection_pool.connection_kwargs['host']}:{source_client.connection_pool.connection_kwargs['port']}"
    target_url = f"redis://{target_client.connection_pool.connection_kwargs['host']}:{target_client.connection_pool.connection_kwargs['port']}"
    
    cmd = [
        "riotx", "replicate",
        source_url,
        target_url,
        "--key-pattern", key_pattern,
        "--struct",
        "--threads", "4",
        "--batch", "500",
        "--progress", "log"
    ]
    
    try:
        print("Starting RIOT replication...")
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            print("RIOT replication completed successfully")
            print(stdout.decode())
        else:
            print("RIOT replication failed")
            print(stderr.decode())
            raise Exception("RIOT replication failed")
            
    except Exception as e:
        print(f"Error during RIOT replication: {e}")
        raise

def run_migration(source_client, target_client, index_name):
    """Main migration process that orchestrates the entire workflow"""
    try:
        # Get index information from source
        index_info = get_index_definition(source_client, index_name)
        if not index_info:
            raise Exception("Failed to retrieve index definition")

        # Extract prefix from source index definition
        prefix = None
        for i in range(0, len(index_info["index_definition"]), 2):
            if index_info["index_definition"][i] == "prefixes" and isinstance(index_info["index_definition"][i + 1], list) and len(index_info["index_definition"][i + 1]) > 0:
                prefix = index_info["index_definition"][i + 1][0]  # Take the first prefix
                break

        if not prefix:
            raise Exception("No prefix found in index definition")

        # 1. Clean up target database
        cleanup_target_database(target_client, index_name, prefix)

        # 2. Create new index in target database
        prefix = recreate_index(target_client, index_info, index_name)

        # 3. Migrate data using RIOT replication
        run_riot_replication(source_client, target_client, f"{prefix}*")

        print("Migration completed successfully!")
        return True

    except Exception as e:
        print(f"Migration failed: {e}")
        return False

if __name__ == "__main__":
    # Initialize Redis connections for source and target databases
    source_client = Redis(host="node1.cluster-kmiller.ps-redis.com", port=17120, decode_responses=True)
    target_client = Redis(host="node1.cluster-kmiller.ps-redis.com", port=12416, decode_responses=True)
    index_name = "docIdx"

    # Run the migration process
    success = run_migration(source_client, target_client, index_name)
    if not success:
        exit(1)