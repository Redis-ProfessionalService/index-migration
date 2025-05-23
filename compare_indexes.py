import redis
from typing import List, Set, Tuple

def get_redis_connection(host: str, port: int) -> redis.Redis:
    """Create and return a Redis connection."""
    return redis.Redis(
        host=host,
        port=port,
        decode_responses=True
    )

def get_indexes(client: redis.Redis) -> Set[str]:
    """Get list of indexes from Redis instance."""
    try:
        return set(client.execute_command('FT._LIST'))
    except redis.RedisError as e:
        print(f"Error getting indexes: {e}")
        return set()

def compare_indexes(source_indexes: Set[str], target_indexes: Set[str]) -> Tuple[Set[str], Set[str], Set[str]]:
    """Compare indexes between source and target.
    
    Returns:
        Tuple containing:
        - indexes only in source
        - indexes only in target
        - indexes in both
    """
    only_in_source = source_indexes - target_indexes
    only_in_target = target_indexes - source_indexes
    in_both = source_indexes & target_indexes
    
    return only_in_source, only_in_target, in_both

def main():
    # Source Redis connection (your original cluster)
    source_client = get_redis_connection(
        host='node1.cluster-kmiller.ps-redis.com',
        port=17120
    )
    
    # Target Redis connection (your new cluster)
    target_client = get_redis_connection(
        host='node1.cluster-kmiller.ps-redis.com',
        port=12416
    )
    
    try:
        # Get indexes from both instances
        source_indexes = get_indexes(source_client)
        target_indexes = get_indexes(target_client)
        
        # Compare the indexes
        only_in_source, only_in_target, in_both = compare_indexes(source_indexes, target_indexes)
        
        # Print results
        print("\nIndex Comparison Results:")
        print("-" * 50)
        
        print("\nIndexes only in source:")
        for idx in sorted(only_in_source):
            print(f"- {idx}")
            
        print("\nIndexes only in target:")
        for idx in sorted(only_in_target):
            print(f"- {idx}")
            
        print("\nIndexes in both source and target:")
        for idx in sorted(in_both):
            print(f"- {idx}")
            
        # Print summary
        print("\nSummary:")
        print(f"Total indexes in source: {len(source_indexes)}")
        print(f"Total indexes in target: {len(target_indexes)}")
        print(f"Indexes only in source: {len(only_in_source)}")
        print(f"Indexes only in target: {len(only_in_target)}")
        print(f"Indexes in both: {len(in_both)}")
        
    finally:
        # Close connections
        source_client.close()
        target_client.close()

if __name__ == '__main__':
    main() 