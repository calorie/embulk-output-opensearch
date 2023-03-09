/*
 * Copyright 2017 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.output.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.json.spi.JsonProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.HttpHost;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.embulk.config.ConfigException;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.PluginTask;
import org.embulk.spi.Exec;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.InfoResponse;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexResponse;
import org.opensearch.client.opensearch.indices.ExistsAliasRequest;
import org.opensearch.client.opensearch.indices.get_alias.IndexAliases;
import org.opensearch.client.opensearch.indices.GetAliasResponse;
import org.opensearch.client.opensearch.indices.GetIndexRequest;
import org.opensearch.client.opensearch.indices.GetIndexResponse;
import org.opensearch.client.opensearch.indices.PutAliasRequest;
import org.opensearch.client.opensearch.indices.PutAliasResponse;
import org.opensearch.client.opensearch.indices.update_aliases.Action;
import org.opensearch.client.opensearch.indices.update_aliases.AddAction;
import org.opensearch.client.opensearch.indices.update_aliases.RemoveAction;
import org.opensearch.client.opensearch.indices.UpdateAliasesRequest;
import org.opensearch.client.opensearch.indices.UpdateAliasesResponse;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.snapshot.SnapshotStatusRequest;
import org.opensearch.client.opensearch.snapshot.SnapshotStatusResponse;
import org.opensearch.client.opensearch.snapshot.Status;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ElasticsearchHttpClient
{
    private final Logger log;

    // ALLOW_UNQUOTED_CONTROL_CHARS - Not expected but whether parser will allow JSON Strings to contain unquoted control characters
    // FAIL_ON_UNKNOWN_PROPERTIES - Feature that determines whether encountering of unknown properties
    private final ObjectMapper jsonMapper = new ObjectMapper()
            .configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false)
            .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // Elasticsearch maximum index byte size
    // public static final int MAX_INDEX_NAME_BYTES = 255;
    // @see https://github.com/elastic/elasticsearch/blob/master/core/src/main/java/org/elasticsearch/cluster/metadata/MetaDataCreateIndexService.java#L108
    private final long maxIndexNameBytes = 255;
    private final List<Character> invalidIndexCharacters = Arrays.asList('\\', '/', '*', '?', '"', '<', '>', '|', '#', ' ', ',');

    public ElasticsearchHttpClient()
    {
        this.log = LoggerFactory.getLogger(getClass());
    }

    public void push(JsonNode records, PluginTask task)
    {
        if (records.size() == 0) {
            return;
        }

        // curl -xPOST localhost:9200/{index}/{type}/_bulk -d '
        // {"index" : {}}\n
        // {"k" : "v"}\n
        // {"index" : {}}\n
        // {"k" : "v2"}\n
        // '
        sendBulkRequest(records, task);
    }

    public List<String> getIndexByAlias(String aliasName, PluginTask task)
    {
        // curl -XGET localhost:9200/_alias/{alias}
        // No alias: 404
        // Alias found: {"embulk_20161018-183738":{"aliases":{"embulk":{}}}}
        GetAliasResponse getAliasResponse = sendGetAliasRequest(aliasName, task);
        Map<String, IndexAliases> result = getAliasResponse.result();
        if (result == null || result.isEmpty()) {
            return new ArrayList<>();
        }

        return result.keySet().stream().collect(Collectors.toList());
    }

    public boolean isIndexExisting(String indexName, PluginTask task)
    {
        // curl -XGET localhost:9200/{index}
        // No index: 404
        // Index found: 200
        try {
            sendGetIndexRequest(indexName, task);
            return true;
        }
        catch (ResourceNotFoundException ex) {
            return false;
        }
    }

    public String generateNewIndexName(String indexName)
    {
        return indexName + new SimpleDateFormat("_yyyyMMdd-HHmmss").format(getTransactionTime().toEpochMilli());
    }

    public boolean isAliasExisting(String aliasName, PluginTask task)
    {
        BooleanResponse booleanResponse = sendExistsAliasRequest(aliasName, task);
        return booleanResponse.value();
    }

    // Should be called just once while Embulk transaction.
    // Be sure to call after all exporting tasks completed
    // This method will delete existing index
    public void reassignAlias(String aliasName, String newIndexName, PluginTask task)
    {
        if (!isAliasExisting(aliasName, task)) {
            assignAlias(newIndexName, aliasName, task);
        }
        else {
            List<String> oldIndices = getIndexByAlias(aliasName, task);
            assignAlias(newIndexName, aliasName, task);
            for (String index : oldIndices) {
                deleteIndex(index, task);
            }
        }
    }

    public String getEsVersion(PluginTask task)
    {
        // curl -XGET 'http://localhost:9200'
        return sendInfoRequest(task).version().number();
    }

    // TODO: delete
    public void validateIndexOrAliasName(String index, String type)
    {
        for (int i = 0; i < index.length(); i++) {
            if (invalidIndexCharacters.contains(index.charAt(i))) {
                throw new ConfigException(String.format("%s '%s' must not contain the invalid characters " + invalidIndexCharacters.toString(), type, index));
            }
        }

        if (!index.toLowerCase(Locale.ROOT).equals(index)) {
            throw new ConfigException(String.format("%s '%s' must be lowercase", type, index));
        }

        if (index.startsWith("_") || index.startsWith("-") || index.startsWith("+")) {
            throw new ConfigException(String.format("%s '%s' must not start with '_', '-', or '+'", type, index));
        }

        if (index.length() > maxIndexNameBytes) {
            throw new ConfigException(String.format("%s name is too long, (%s > %s)", type, index.length(), maxIndexNameBytes));
        }

        if (index.equals(".") || index.equals("..")) {
            throw new ConfigException("index must not be '.' or '..'");
        }
    }

    private Optional<String> getRecordId(JsonNode record, Optional<String> idColumn)
    {
        if (idColumn.isPresent() && record.hasNonNull(idColumn.get())) {
            return Optional.of(record.get(idColumn.get()).asText());
        }

        return Optional.empty();
    }

    private void assignAlias(String indexName, String aliasName, PluginTask task)
    {
        if (!isIndexExisting(indexName, task)) {
            return;
        }

        if (!isAliasExisting(aliasName, task)) {
            // curl -XPUT http://localhost:9200/{index}/_alias/{alias}
            // Success: {"acknowledged":true}
            sendPutAliasRequest(indexName, aliasName, task);
            log.info("Assigned alias [{}] to Index [{}]", aliasName, indexName);

            return;
        }

        // curl -XPUT http://localhost:9200/_alias -d\
        // "actions" : [
        //   {"remove" : {"alias" : "{alias}", "index" : "{index_old}"}},
        //   {"add" : {"alias": "{alias}", "index": "{index_new}"}}
        // ]
        // Success: {"acknowledged":true}
        List<String> oldIndices = getIndexByAlias(aliasName, task);
        sendUpdateAliasesRequest(oldIndices, indexName, aliasName, task);

        log.info("Reassigned alias [{}] to index[{}]", aliasName, indexName);
    }

    private void deleteIndex(String indexName, PluginTask task)
    {
        if (!isIndexExisting(indexName, task)) {
            return;
        }

        waitSnapshot(task);

        // curl -XDELETE localhost:9200/{index}
        // Success: {"acknowledged":true}
        sendDeleteIndexRequest(indexName, task);

        log.info("Deleted Index [{}]", indexName);
    }

    private void waitSnapshot(PluginTask task)
    {
        int maxSnapshotWaitingMills = task.getMaxSnapshotWaitingSecs() * 1000;
        long execCount = 1;
        long totalWaitingTime = 0;
        // Since only needs exponential backoff, don't need exception handling and others, I don't use Embulk RetryExecutor
        while (isSnapshotProgressing(task)) {
            long sleepTime = ((long) Math.pow(2, execCount) * 1000);
            try {
                Thread.sleep(sleepTime);
            }
            catch (InterruptedException ex) {
                // do nothing
            }
            if (execCount > 1) {
                log.info("Waiting for snapshot completed.");
            }
            execCount++;
            totalWaitingTime += sleepTime;
            if (totalWaitingTime > maxSnapshotWaitingMills) {
                throw new ConfigException(String.format("Waiting creating snapshot is expired. %s sec.", maxSnapshotWaitingMills));
            }
        }
    }

    private boolean isSnapshotProgressing(PluginTask task)
    {
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html#_snapshot_status
        // curl -XGET localhost:9200/_snapshot/_status
        SnapshotStatusResponse snapshotStatusResponse = sendSnapshotStatusRequest(task);
        List<Status> snapshots = snapshotStatusResponse.snapshots();

        return snapshots != null && !snapshots.isEmpty();
    }

    private BulkResponse sendBulkRequest(JsonNode records, PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper(task)) {
            JsonpMapper jsonpMapper = retryHelper.jsonpMapper();
            JsonProvider jsonProvider = retryHelper.jsonProvider();
            Optional<String> idColumn = task.getId();
            BulkRequest.Builder br = new BulkRequest.Builder();

            for (JsonNode record : records) {
                // TODO: performance
                JsonData jsonData = parseRecord(record, jsonpMapper, jsonProvider);
                Optional<String> id = getRecordId(record, idColumn);

                br.operations(op -> op
                    .index(idx -> idx
                        .index(task.getIndex())
                        .id(id.orElse(null))
                        .document(jsonData)
                    )
                );
            }
            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester() {
                        @Override
                        public <T> T requestOnce(org.opensearch.client.opensearch.OpenSearchClient client, final Class<T> clazz)
                        {
                            try {
                                // TODO: no cast
                                return clazz.cast(client.bulk(br.build()));
                            }
                            catch (IOException e) {
                                // TODO
                                throw new RuntimeException(e);
                            }
                        }

                        @Override
                        protected boolean isExceptionToRetry(Exception exception)
                        {
                            return task.getId().isPresent();
                        }
                    }, BulkResponse.class);
        }
    }

    private GetAliasResponse sendGetAliasRequest(String aliasName, PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper(task)) {
            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester() {
                        @Override
                        public <T> T requestOnce(org.opensearch.client.opensearch.OpenSearchClient client, final Class<T> clazz)
                        {
                            try {
                                // TODO: no cast
                                return clazz.cast(client.indices().getAlias(a -> a.name(aliasName)));
                            }
                            catch (IOException e) {
                                // TODO
                                throw new RuntimeException(e);
                            }
                        }
                    }, GetAliasResponse.class);
        }
    }

    private GetIndexResponse sendGetIndexRequest(String indexName, PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper(task)) {
            GetIndexRequest request = new GetIndexRequest.Builder().index(indexName).build();

            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester() {
                        @Override
                        public <T> T requestOnce(org.opensearch.client.opensearch.OpenSearchClient client, final Class<T> clazz)
                        {
                            try {
                                // TODO: no cast
                                return clazz.cast(client.indices().get(request));
                            }
                            catch (IOException e) {
                                // TODO
                                throw new RuntimeException(e);
                            }
                        }
                    }, GetIndexResponse.class);
        }
    }

    private BooleanResponse sendExistsAliasRequest(String aliasName, PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper(task)) {
            ExistsAliasRequest request = new ExistsAliasRequest.Builder().name(aliasName).build();

            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester() {
                        @Override
                        public <T> T requestOnce(org.opensearch.client.opensearch.OpenSearchClient client, final Class<T> clazz)
                        {
                            try {
                                // TODO: no cast
                                return clazz.cast(client.indices().existsAlias(request));
                            }
                            catch (IOException e) {
                                // TODO
                                throw new RuntimeException(e);
                            }
                        }
                    }, BooleanResponse.class);
        }
    }

    private InfoResponse sendInfoRequest(final PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper(task)) {
            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester() {
                        @Override
                        public <T> T requestOnce(org.opensearch.client.opensearch.OpenSearchClient client, final Class<T> clazz)
                        {
                            try {
                                // TODO: no cast
                                return clazz.cast(client.info());
                            }
                            catch (IOException e) {
                                // TODO
                                throw new RuntimeException(e);
                            }
                        }
                    }, InfoResponse.class);
        }
    }

    private PutAliasResponse sendPutAliasRequest(final String indexName, final String aliasName, final PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper(task)) {
            PutAliasRequest request = new PutAliasRequest.Builder().index(indexName).name(aliasName).build();

            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester() {
                        @Override
                        public <T> T requestOnce(org.opensearch.client.opensearch.OpenSearchClient client, final Class<T> clazz)
                        {
                            try {
                                // TODO: no cast
                                return clazz.cast(client.indices().putAlias(request));
                            }
                            catch (IOException e) {
                                // TODO
                                throw new RuntimeException(e);
                            }
                        }
                    }, PutAliasResponse.class);
        }
    }

    private UpdateAliasesResponse sendUpdateAliasesRequest(final List<String> oldIndices, final String indexName, final String aliasName, final PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper(task)) {
            UpdateAliasesRequest.Builder br = new UpdateAliasesRequest.Builder();
            br.actions(ac -> ac.remove(ra -> ra.alias(aliasName).indices(oldIndices)));
            br.actions(ac -> ac.add(aa -> aa.alias(aliasName).index(indexName)));

            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester() {
                        @Override
                        public <T> T requestOnce(org.opensearch.client.opensearch.OpenSearchClient client, final Class<T> clazz)
                        {
                            try {
                                // TODO: no cast
                                return clazz.cast(client.indices().updateAliases(br.build()));
                            }
                            catch (IOException e) {
                                // TODO
                                throw new RuntimeException(e);
                            }
                        }
                    }, UpdateAliasesResponse.class);
        }
    }

    private SnapshotStatusResponse sendSnapshotStatusRequest(final PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper(task)) {
            SnapshotStatusRequest request = new SnapshotStatusRequest.Builder().build();

            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester() {
                        @Override
                        public <T> T requestOnce(org.opensearch.client.opensearch.OpenSearchClient client, final Class<T> clazz)
                        {
                            try {
                                // TODO: no cast
                                return clazz.cast(client.snapshot().status(request));
                            }
                            catch (IOException e) {
                                // TODO
                                throw new RuntimeException(e);
                            }
                        }
                    }, SnapshotStatusResponse.class);
        }
    }

    private DeleteIndexResponse sendDeleteIndexRequest(final String indexName, final PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper(task)) {
            DeleteIndexRequest request = new DeleteIndexRequest.Builder().index(indexName).build();

            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester() {
                        @Override
                        public <T> T requestOnce(org.opensearch.client.opensearch.OpenSearchClient client, final Class<T> clazz)
                        {
                            try {
                                // TODO: no cast
                                return clazz.cast(client.indices().delete(request));
                            }
                            catch (IOException e) {
                                // TODO
                                throw new RuntimeException(e);
                            }
                        }
                    }, DeleteIndexResponse.class);
        }
    }

    private OpenSearchRetryHelper createRetryHelper(final PluginTask task)
    {
        return new OpenSearchRetryHelper(
                task.getMaximumRetries(),
                task.getInitialRetryIntervalMillis(),
                task.getMaximumRetryIntervalMillis(),
                new OpenSearchClientCreator() {
                    @Override
                    public org.opensearch.client.opensearch.OpenSearchClient createAndStart()
                    {
                        RestClient restClient = null;
                        try {
                            // TODO: secret, authorization, timeout
                            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(AuthScope.ANY,
                                new UsernamePasswordCredentials("admin", "admin"));

                            restClient = RestClient.builder(new HttpHost("opensearch", 9200, "http")).
                              setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                                @Override
                                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder)
                                {
                                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                                }
                              }).build();
                            OpenSearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
                            return new OpenSearchClient(transport);
                        }
                        catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                });
    }

    private JsonData parseRecord(JsonNode record, JsonpMapper jsonpMapper, JsonProvider jsonProvider)
    {
        return JsonData.from(jsonProvider.createParser(new StringReader(record.toString())), jsonpMapper);
    }

    @SuppressWarnings("deprecation")
    private static Instant getTransactionTime()
    {
        if (HAS_EXEC_GET_TRANSACTION_TIME_INSTANT) {
            return Exec.getTransactionTimeInstant();
        }
        return Exec.getTransactionTime().getInstant();
    }

    private static boolean hasExecGetTransactionTimeInstant()
    {
        try {
            Exec.class.getMethod("getTransactionTimeInstant");
        }
        catch (final NoSuchMethodException ex) {
            return false;
        }
        return true;
    }

    private static final boolean HAS_EXEC_GET_TRANSACTION_TIME_INSTANT = hasExecGetTransactionTimeInstant();
}
