import redis
from typing import List, Set, Tuple, Dict
import time
import argparse

def get_redis_connection(host: str, port: int) -> redis.Redis:
    """Create and return a Redis connection."""
    return redis.Redis(
        host=host,
        port=port,
        decode_responses=True
    )

def get_keys_by_pattern(client: redis.Redis, pattern: str = "*") -> Set[str]:
    """Get all keys matching the pattern from Redis instance."""
    try:
        cursor = 0
        keys = set()
        while True:
            cursor, partial_keys = client.scan(cursor, match=pattern, count=1000)
            keys.update(partial_keys)
            if cursor == 0:
                break
        return keys
    except redis.RedisError as e:
        print(f"Error getting keys: {e}")
        return set()

def compare_keys(source_keys: Set[str], target_keys: Set[str]) -> Tuple[Set[str], Set[str], Set[str]]:
    """Compare keys between source and target.
    
    Returns:
        Tuple containing:
        - keys only in source
        - keys only in target
        - keys in both
    """
    only_in_source = source_keys - target_keys
    only_in_target = target_keys - source_keys
    in_both = source_keys & target_keys
    
    return only_in_source, only_in_target, in_both

def get_key_type(client: redis.Redis, key: str) -> str:
    """Get the type of a Redis key."""
    try:
        return client.type(key)
    except redis.RedisError:
        return "unknown"

def analyze_key_patterns(keys: Set[str]) -> Dict[str, int]:
    """Analyze key patterns and count occurrences."""
    patterns = {}
    for key in keys:
        # Extract the prefix (everything before the first colon)
        prefix = key.split(':')[0] if ':' in key else key
        patterns[prefix] = patterns.get(prefix, 0) + 1
    return patterns

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Compare Redis keys between source and target instances')
    parser.add_argument('--debug', action='store_true', help='Enable detailed debug output')
    args = parser.parse_args()

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
        print("Scanning source Redis instance...")
        start_time = time.time()
        source_keys = get_keys_by_pattern(source_client)
        source_scan_time = time.time() - start_time
        
        print("Scanning target Redis instance...")
        start_time = time.time()
        target_keys = get_keys_by_pattern(target_client)
        target_scan_time = time.time() - start_time
        
        # Compare the keys
        only_in_source, only_in_target, in_both = compare_keys(source_keys, target_keys)
        
        # Analyze key patterns
        source_patterns = analyze_key_patterns(source_keys)
        target_patterns = analyze_key_patterns(target_keys)
        
        # Print results
        print("\nKey Comparison Results:")
        print("-" * 50)
        
        if args.debug:
            print("\nKeys only in source:")
            for key in sorted(only_in_source)[:10]:  # Show first 10 keys
                key_type = get_key_type(source_client, key)
                print(f"- {key} (Type: {key_type})")
            if len(only_in_source) > 10:
                print(f"... and {len(only_in_source) - 10} more keys")
                
            print("\nKeys in target:")
            for key in sorted(target_keys):  # Show all keys
                key_type = get_key_type(target_client, key)
                print(f"- {key} (Type: {key_type})")
        
        # Print summary
        print("\nSummary:")
        print(f"Total keys in source: {len(source_keys)}")
        print(f"Total keys in target: {len(target_keys)}")
        print(f"Keys only in source: {len(only_in_source)}")
        print(f"Keys only in target: {len(only_in_target)}")
        print(f"Keys in both: {len(in_both)}")
        
        print("\nScan Times:")
        print(f"Source scan time: {source_scan_time:.2f} seconds")
        print(f"Target scan time: {target_scan_time:.2f} seconds")
        
        print("\nKey Pattern Analysis:")
        
        if args.debug: 
            print("\nSource Redis Key Patterns:")
            for pattern, count in sorted(source_patterns.items(), key=lambda x: x[1], reverse=True):
                print(f"- {pattern}: {count} keys")
    
        print("\nTarget Redis Key Patterns:")
        for pattern, count in sorted(target_patterns.items(), key=lambda x: x[1], reverse=True):
            print(f"- {pattern}: {count} keys")
        
    finally:
        # Close connections
        source_client.close()
        target_client.close()

if __name__ == '__main__':
    main() 