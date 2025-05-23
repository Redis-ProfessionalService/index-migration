## Pre requisties:
- Both the source and target database exist
- Keyspace notifications on the Redis source instance


# Steps
1. Create the index

2. Replicate the database
```bash
./riotx replicate redis://node1.cluster-kmiller.ps-redis.com:17120 redis://node1.cluster-kmiller.ps-redis.com:12416 \
  --keys "vector:doc:*" \
  --mode live \
  --struct \
  --threads 4 \
  --batch 500 \
  --progress log
  ```

### Notes:
-  riot replicate doesn't bring indexes, Index needs to be created outside of RIOT.   
    - _It doesn't maer which came first, the document or the index. **FT.CREATE** will index any new or existing documents that meet the conditions. In this example, a hash that has a key starting with employee._

