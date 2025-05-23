# Redis Index Migration Tool

This tool facilitates the migration of Redis vector indices and their associated data from a source Redis database to a target Redis database. It uses RedisVL (Redis Vector Library) for index management and RIOT (Redis Input/Output Tools) for data replication.

## Features

- Migrates Redis vector indices with their complete schema
- Handles TEXT, NUMERIC, and VECTOR field types
- Preserves index configuration including prefixes and field attributes
- Uses RIOT for efficient data replication
- Supports cleanup of target database before migration
- Maintains vector field properties (algorithm, dimensions, distance metric, data type)

## Prerequisites

- Python 3.x
- Redis-py
- RedisVL
- RIOT (Redis Input/Output Tools)

## Installation

1. Install the required Python packages:
```bash
pip install -r requirements.txt
```

2. Install RIOT (if not already installed):
```bash
# For macOS
brew install redis-stack/redis-stack/riot

# For Linux
# Download from https://github.com/redis-developer/riot/releases
```

## Usage

1. Configure the source and target Redis connections in the script:
```python
source_client = Redis(host="source_host", port=source_port, decode_responses=True)
target_client = Redis(host="target_host", port=target_port, decode_responses=True)
index_name = "your_index_name"
```

2. Run the migration script:
```bash
python migrate_index_redisvl.py
```

## Migration Process

The script performs the following steps:

1. Retrieves the index definition from the source database
2. Cleans up the target database (removes existing index and matching keys)
3. Recreates the index in the target database with the same schema
4. Migrates the data using RIOT replication

## Error Handling

The script includes comprehensive error handling for:
- Index retrieval failures
- Database cleanup issues
- Index recreation problems
- Data replication errors

## Notes

- The script uses RIOT with the following default settings:
  - 4 threads
  - Batch size of 500
  - Progress logging enabled
- Vector fields are migrated with their original configuration (algorithm, dimensions, distance metric, data type)
- The script preserves the original index prefix pattern

## Troubleshooting

If you encounter issues:

1. Verify Redis connectivity to both source and target databases
2. Ensure RIOT is properly installed and accessible in your PATH
3. Check that the index name exists in the source database
4. Verify sufficient permissions on both Redis instances
5. Check the console output for specific error messages
---
_Copyright (c) 2025 Redis Ltd. All rights reserved.
  This material is provided under the terms of your Professional Services agreement or Statement of Work (SOW) with Redis Ltd. and subject to the applicable Redis customer agreement. Unless otherwise agreed in writing, you are granted a limited, non-exclusive, non-sublicensable, non-transferable, revocable license to use this material solely for your internal operations in connection with the use of Redis services._