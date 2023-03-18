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

package org.embulk.output.opensearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.json.JsonObject;
import jakarta.json.JsonValue;
import jakarta.json.stream.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.embulk.config.ConfigException;
import org.embulk.output.opensearch.OpenSearchOutputPluginDelegate.AuthMethod;
import org.embulk.output.opensearch.OpenSearchOutputPluginDelegate.NodeAddressTask;
import org.embulk.output.opensearch.OpenSearchOutputPluginDelegate.PluginTask;
import org.embulk.spi.Exec;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.json.jackson.JacksonJsonpParser;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.InfoResponse;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexResponse;
import org.opensearch.client.opensearch.indices.ExistsAliasRequest;
import org.opensearch.client.opensearch.indices.GetAliasResponse;
import org.opensearch.client.opensearch.indices.GetIndexRequest;
import org.opensearch.client.opensearch.indices.GetIndexResponse;
import org.opensearch.client.opensearch.indices.PutAliasRequest;
import org.opensearch.client.opensearch.indices.PutAliasResponse;
import org.opensearch.client.opensearch.indices.UpdateAliasesRequest;
import org.opensearch.client.opensearch.indices.UpdateAliasesResponse;
import org.opensearch.client.opensearch.indices.get_alias.IndexAliases;
import org.opensearch.client.opensearch.snapshot.SnapshotStatusRequest;
import org.opensearch.client.opensearch.snapshot.SnapshotStatusResponse;
import org.opensearch.client.opensearch.snapshot.Status;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.TransportOptions;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.client.transport.rest_client.RestClientTransport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class OpenSearchHttpClient
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

    public OpenSearchHttpClient()
    {
        this.log = LoggerFactory.getLogger(getClass());
    }

    public void push(final JsonNode records, final PluginTask task)
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

    public boolean isIndexExisting(final String indexName, final PluginTask task)
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

    public String generateNewIndexName(final String indexName)
    {
        return indexName + new SimpleDateFormat("_yyyyMMdd-HHmmss").format(getTransactionTime().toEpochMilli());
    }

    public boolean isAliasExisting(final String aliasName, final PluginTask task)
    {
        final BooleanResponse booleanResponse = sendExistsAliasRequest(aliasName, task);
        return booleanResponse.value();
    }

    // Should be called just once while Embulk transaction.
    // Be sure to call after all exporting tasks completed
    // This method will delete existing index
    public void reassignAlias(final String aliasName, final String newIndexName, final PluginTask task)
    {
        if (!isAliasExisting(aliasName, task)) {
            assignAlias(newIndexName, aliasName, task);
        }
        else {
            final List<String> oldIndices = getIndexByAlias(aliasName, task);
            assignAlias(newIndexName, aliasName, task);
            for (final String index : oldIndices) {
                deleteIndex(index, task);
            }
        }
    }

    public String getEsVersion(final PluginTask task)
    {
        // curl -XGET 'http://localhost:9200'
        return sendInfoRequest(task).version().number();
    }

    // TODO: Delete this
    public void validateIndexOrAliasName(final String index)
    {
        for (int i = 0; i < index.length(); i++) {
            if (invalidIndexCharacters.contains(index.charAt(i))) {
                throw new ConfigException(String.format("'%s' must not contain the invalid characters " + invalidIndexCharacters.toString(), index));
            }
        }

        if (!index.toLowerCase(Locale.ROOT).equals(index)) {
            throw new ConfigException(String.format("'%s' must be lowercase", index));
        }

        if (index.startsWith("_") || index.startsWith("-") || index.startsWith("+")) {
            throw new ConfigException(String.format("'%s' must not start with '_', '-', or '+'", index));
        }

        if (index.length() > maxIndexNameBytes) {
            throw new ConfigException(String.format("index name is too long, (%s > %s)", index.length(), maxIndexNameBytes));
        }

        if (index.equals(".") || index.equals("..")) {
            throw new ConfigException("index must not be '.' or '..'");
        }
    }

    private List<String> getIndexByAlias(final String aliasName, final PluginTask task)
    {
        // curl -XGET localhost:9200/_alias/{alias}
        // No alias: 404
        // Alias found: {"embulk_20161018-183738":{"aliases":{"embulk":{}}}}
        final GetAliasResponse getAliasResponse = sendGetAliasRequest(aliasName, task);
        final Map<String, IndexAliases> result = getAliasResponse.result();
        if (result == null || result.isEmpty()) {
            return new ArrayList<>();
        }

        return result.keySet().stream().collect(Collectors.toList());
    }

    private Optional<String> getRecordId(final JsonObject record, final Optional<String> idColumn)
    {
        if (!idColumn.isPresent()) {
            return Optional.empty();
        }

        final String id = record.getString(idColumn.get(), null);

        return (id == null) ? Optional.empty() : Optional.of(id);
    }

    private void assignAlias(final String indexName, final String aliasName, final PluginTask task)
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
        final List<String> oldIndices = getIndexByAlias(aliasName, task);
        sendUpdateAliasesRequest(oldIndices, indexName, aliasName, task);

        log.info("Reassigned alias [{}] to index[{}]", aliasName, indexName);
    }

    private void deleteIndex(final String indexName, final PluginTask task)
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

    private void waitSnapshot(final PluginTask task)
    {
        final int maxSnapshotWaitingMills = task.getMaxSnapshotWaitingSecs() * 1000;
        long execCount = 1;
        long totalWaitingTime = 0;
        // Since only needs exponential backoff, don't need exception handling and others, I don't use Embulk RetryExecutor
        while (isSnapshotProgressing(task)) {
            final long sleepTime = ((long) Math.pow(2, execCount) * 1000);
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

    private boolean isSnapshotProgressing(final PluginTask task)
    {
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html#_snapshot_status
        // curl -XGET localhost:9200/_snapshot/_status
        final SnapshotStatusResponse snapshotStatusResponse = sendSnapshotStatusRequest(task);
        final List<Status> snapshots = snapshotStatusResponse.snapshots();

        return snapshots != null && !snapshots.isEmpty();
    }

    private BulkResponse sendBulkRequest(final JsonNode records, final PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper(task)) {
            BulkRequest.Builder br = new BulkRequest.Builder();

            final JsonParser parser = new JacksonJsonpParser(records.traverse());
            final JsonData jsonData = JsonData.from(parser, retryHelper.jsonpMapper());

            for (final JsonValue jsonValue : jsonData.toJson().asJsonArray()) {
                final JsonObject record = jsonValue.asJsonObject();
                final Optional<String> id = getRecordId(record, task.getId());

                br.operations(op -> op
                    .index(idx -> idx
                        .index(task.getIndex())
                        .id(id.orElse(null))
                        .document(record)
                    )
                );
            }
            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester<BulkResponse>() {
                        @Override
                        public BulkResponse requestOnce(org.opensearch.client.opensearch.OpenSearchClient client)
                        {
                            try {
                                final TransportOptions filterOptions = client._transport().options().with(b -> b
                                    .setParameter("filter_path", "-took,-items.index._*")
                                );
                                return client.withTransportOptions(filterOptions).bulk(br.build());
                            }
                            catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        @Override
                        protected boolean isExceptionToRetry(final Exception exception)
                        {
                            return task.getId().isPresent();
                        }
                    });
        }
    }

    private GetAliasResponse sendGetAliasRequest(final String aliasName, final PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper(task)) {
            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester<GetAliasResponse>() {
                        @Override
                        public GetAliasResponse requestOnce(org.opensearch.client.opensearch.OpenSearchClient client)
                        {
                            try {
                                return client.indices().getAlias(a -> a.name(aliasName));
                            }
                            catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
        }
    }

    private GetIndexResponse sendGetIndexRequest(final String indexName, final PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper(task)) {
            GetIndexRequest request = new GetIndexRequest.Builder().index(indexName).build();

            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester<GetIndexResponse>() {
                        @Override
                        public GetIndexResponse requestOnce(org.opensearch.client.opensearch.OpenSearchClient client)
                        {
                            try {
                                return client.indices().get(request);
                            }
                            catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
        }
    }

    private BooleanResponse sendExistsAliasRequest(final String aliasName, final PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper(task)) {
            ExistsAliasRequest request = new ExistsAliasRequest.Builder().name(aliasName).build();

            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester<BooleanResponse>() {
                        @Override
                        public BooleanResponse requestOnce(org.opensearch.client.opensearch.OpenSearchClient client)
                        {
                            try {
                                return client.indices().existsAlias(request);
                            }
                            catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
        }
    }

    private InfoResponse sendInfoRequest(final PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper(task)) {
            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester<InfoResponse>() {
                        @Override
                        public InfoResponse requestOnce(org.opensearch.client.opensearch.OpenSearchClient client)
                        {
                            try {
                                return client.info();
                            }
                            catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
        }
    }

    private PutAliasResponse sendPutAliasRequest(final String indexName, final String aliasName, final PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper(task)) {
            PutAliasRequest request = new PutAliasRequest.Builder().index(indexName).name(aliasName).build();

            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester<PutAliasResponse>() {
                        @Override
                        public PutAliasResponse requestOnce(org.opensearch.client.opensearch.OpenSearchClient client)
                        {
                            try {
                                return client.indices().putAlias(request);
                            }
                            catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
        }
    }

    private UpdateAliasesResponse sendUpdateAliasesRequest(final List<String> oldIndices, final String indexName, final String aliasName, final PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper(task)) {
            UpdateAliasesRequest.Builder br = new UpdateAliasesRequest.Builder();

            br.actions(ac -> ac.remove(ra -> ra.alias(aliasName).indices(oldIndices)));
            br.actions(ac -> ac.add(aa -> aa.alias(aliasName).index(indexName)));

            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester<UpdateAliasesResponse>() {
                        @Override
                        public UpdateAliasesResponse requestOnce(org.opensearch.client.opensearch.OpenSearchClient client)
                        {
                            try {
                                return client.indices().updateAliases(br.build());
                            }
                            catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
        }
    }

    private SnapshotStatusResponse sendSnapshotStatusRequest(final PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper(task)) {
            SnapshotStatusRequest request = new SnapshotStatusRequest.Builder().build();

            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester<SnapshotStatusResponse>() {
                        @Override
                        public SnapshotStatusResponse requestOnce(org.opensearch.client.opensearch.OpenSearchClient client)
                        {
                            try {
                                return client.snapshot().status(request);
                            }
                            catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
        }
    }

    private DeleteIndexResponse sendDeleteIndexRequest(final String indexName, final PluginTask task)
    {
        try (OpenSearchRetryHelper retryHelper = createRetryHelper(task)) {
            DeleteIndexRequest request = new DeleteIndexRequest.Builder().index(indexName).build();

            return retryHelper.requestWithRetry(
                    new OpenSearchSingleRequester<DeleteIndexResponse>() {
                        @Override
                        public DeleteIndexResponse requestOnce(org.opensearch.client.opensearch.OpenSearchClient client)
                        {
                            try {
                                return client.indices().delete(request);
                            }
                            catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
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
                        try {
                            RestClientBuilder restClientBuilder = RestClient.builder(getHttpHosts(task).toArray(new HttpHost[0]));

                            restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                                @Override
                                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder)
                                {
                                    if (task.getAuthMethod() == AuthMethod.BASIC) {
                                        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                                        credentialsProvider.setCredentials(AuthScope.ANY,
                                            new UsernamePasswordCredentials(task.getUser().get(), task.getPassword().get()));
                                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                                    }

                                    return httpClientBuilder;
                                }
                            });

                            restClientBuilder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                                @Override
                                public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder)
                                {
                                    requestConfigBuilder.setConnectTimeout(task.getConnectTimeoutMills());
                                    requestConfigBuilder.setConnectionRequestTimeout(task.getTimeoutMills());
                                    return requestConfigBuilder;
                                }
                            });

                            final OpenSearchTransport transport = new RestClientTransport(restClientBuilder.build(), new JacksonJsonpMapper());
                            return new OpenSearchClient(transport);
                        }
                        catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                });
    }

    private List<HttpHost> getHttpHosts(final PluginTask task)
    {
        List<HttpHost> hosts = new ArrayList<>();
        final String protocol = task.getUseSsl() ? "https" : "http";
        for (NodeAddressTask node : task.getNodes()) {
            hosts.add(new HttpHost(node.getHost(), node.getPort(), protocol));
        }
        return hosts;
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