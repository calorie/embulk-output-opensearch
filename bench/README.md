## Benchmark

```sh
docker compose build --build-arg PACKAGE_USERNAME=$PACKAGE_USERNAME --build-arg PACKAGE_TOKEN=$PACKAGE_TOKEN embulk
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
