## Benchmark

```sh
docker compose build
ruby jsonl.rb
```

### OpenSearch

```sh
docker compose up opensearch
docker compose run --rm embulk java -cp "/jar/*" org.embulk.cli.Main run -b /embulk /bench/opensearch.yml
```

### Elasticsearch

```sh
docker compose up elasticsearch
docker compose run --rm embulk java -cp "/jar/*" org.embulk.cli.Main run -b /embulk /bench/elasticsearch.yml
```
