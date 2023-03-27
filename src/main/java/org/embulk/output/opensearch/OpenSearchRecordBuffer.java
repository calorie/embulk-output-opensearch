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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.config.TaskReport;
import org.embulk.output.opensearch.OpenSearchOutputPluginDelegate.PluginTask;
import org.embulk.output.opensearch.jackson.JacksonServiceRecord;
import org.embulk.output.opensearch.jackson.JacksonServiceValue;
import org.embulk.output.opensearch.jackson.JacksonTopLevelValueLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OpenSearchRecordBuffer is an implementation of {@code RecordBuffer} which includes JSON output directly to OpenSearch server.
 */
public class OpenSearchRecordBuffer extends RecordBuffer
{
    private final PluginTask task;
    private final long bulkActions;
    private final long bulkSize;
    private final OpenSearchHttpClient client;
    private final Logger log;
    private long totalCount;
    private int requestCount;
    private long requestBytes;
    private ArrayNode records;

    public OpenSearchRecordBuffer(final String attributeName, final PluginTask task)
    {
        this.task = task;
        this.bulkActions = task.getBulkActions();
        this.bulkSize = task.getBulkSize();
        this.client = new OpenSearchHttpClient();
        this.records = JsonNodeFactory.instance.arrayNode();
        this.totalCount = 0;
        this.requestCount = 0;
        this.requestBytes = 0;
        this.log = LoggerFactory.getLogger(getClass());
    }

    @Override
    public void bufferRecord(ServiceRecord serviceRecord)
    {
        try {
            final JacksonServiceRecord jacksonServiceRecord = (JacksonServiceRecord) serviceRecord;

            final JacksonTopLevelValueLocator locator = new JacksonTopLevelValueLocator("record");
            final JacksonServiceValue serviceValue = jacksonServiceRecord.getValue(locator);
            final JsonNode record = serviceValue.getInternalJsonNode();

            records.add(record);

            requestCount++;
            totalCount++;
            requestBytes += record.toString().getBytes().length;

            if (bulkActions <= requestCount || bulkSize <= requestBytes) {
                client.push(records, task);

                if (totalCount % 10000 == 0) {
                    log.info("Inserted {} records", totalCount);
                }

                records = JsonNodeFactory.instance.arrayNode();
                requestBytes = 0;
                requestCount = 0;
            }
        }
        catch (ClassCastException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void finish()
    {
    }

    @Override
    public void close()
    {
    }

    @Override
    public TaskReport commitWithTaskReportUpdated(TaskReport taskReport)
    {
        if (records.size() > 0) {
            client.push(records, task);
            log.info("Inserted {} records", records.size());
        }

        return OpenSearchOutputPlugin.CONFIG_MAPPER_FACTORY.newTaskReport().set("inserted", totalCount);
    }
}
