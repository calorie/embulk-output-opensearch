/*
 * Copyright 2015 The Embulk project
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

import com.google.common.collect.Lists;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.output.opensearch.OpenSearchOutputPluginDelegate.AuthMethod;
import org.embulk.output.opensearch.OpenSearchOutputPluginDelegate.Mode;
import org.embulk.output.opensearch.OpenSearchOutputPluginDelegate.PluginTask;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.SearchResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.embulk.output.opensearch.OpenSearchTestUtils.ES_BULK_ACTIONS;
import static org.embulk.output.opensearch.OpenSearchTestUtils.ES_BULK_SIZE;
import static org.embulk.output.opensearch.OpenSearchTestUtils.ES_ID;
import static org.embulk.output.opensearch.OpenSearchTestUtils.ES_INDEX;
import static org.embulk.output.opensearch.OpenSearchTestUtils.ES_NODES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;

public class TestOpenSearchOutputPlugin
{
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = OpenSearchOutputPlugin.CONFIG_MAPPER_FACTORY;
    private static final ConfigMapper CONFIG_MAPPER = OpenSearchOutputPlugin.CONFIG_MAPPER;

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private OpenSearchOutputPlugin plugin;
    private OpenSearchTestUtils utils;
    private OpenSearchClient openSearchClient;

    @Before
    public void createResources() throws Exception
    {
        utils = new OpenSearchTestUtils();
        utils.initializeConstant();
        final PluginTask task = CONFIG_MAPPER.map(utils.config(), PluginTask.class);
        utils.prepareBeforeTest(task);

        plugin = new OpenSearchOutputPlugin();

        openSearchClient = utils.client();
    }

    @After
    public void close()
    {
        try {
            openSearchClient._transport().close();
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testDefaultValues()
    {
        final PluginTask task = CONFIG_MAPPER.map(utils.config(), PluginTask.class);
        assertThat(task.getIndex(), is(ES_INDEX));
    }

    @Test
    public void testDefaultValuesNull()
    {
        final ConfigSource config = runtime.getExec().newConfigSource()
            .set("in", utils.inputConfig())
            .set("parser", utils.parserConfig(utils.schemaConfig()))
            .set("type", "opensearch")
            .set("mode", "") // NULL
            .set("nodes", ES_NODES)
            .set("index", ES_INDEX)
            .set("id", ES_ID)
            .set("bulk_actions", ES_BULK_ACTIONS)
            .set("bulk_size", ES_BULK_SIZE);
        Schema schema = CONFIG_MAPPER.map(utils.oldParserConfig(runtime), CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        try {
            plugin.transaction(config, schema, 0, new OutputPlugin.Control()
            {
                @Override
                public List<TaskReport> run(TaskSource taskSource)
                {
                    return Lists.newArrayList(CONFIG_MAPPER_FACTORY.newTaskReport());
                }
            });
        }
        catch (Throwable t) {
            if (t instanceof RuntimeException) {
                assertTrue(t instanceof ConfigException);
            }
        }
    }

    @Test
    public void testTransaction()
    {
        ConfigSource config = utils.config();
        Schema schema = CONFIG_MAPPER.map(utils.oldParserConfig(runtime), CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        plugin.transaction(config, schema, 0, new OutputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(CONFIG_MAPPER_FACTORY.newTaskReport());
            }
        });
        // no error happens
    }

    @Test
    public void testResume()
    {
        ConfigSource config = utils.config();
        Schema schema = CONFIG_MAPPER.map(utils.oldParserConfig(runtime), CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        plugin.resume(task.toTaskSource(), schema, 0, new OutputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(CONFIG_MAPPER_FACTORY.newTaskReport());
            }
        });
    }

    @Test
    public void testCleanup()
    {
        ConfigSource config = utils.config();
        Schema schema = CONFIG_MAPPER.map(utils.oldParserConfig(runtime), CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        plugin.cleanup(task.toTaskSource(), schema, 0, Arrays.asList(CONFIG_MAPPER_FACTORY.newTaskReport()));
        // no error happens
    }

    @Test
    public void testOutputByOpen() throws Exception
    {
        ConfigSource config = utils.config();
        Schema schema = CONFIG_MAPPER.map(utils.oldParserConfig(runtime), CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        plugin.transaction(config, schema, 0, new OutputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(CONFIG_MAPPER_FACTORY.newTaskReport());
            }
        });
        TransactionalPageOutput output = plugin.open(task.toTaskSource(), schema, 0);

        List<Page> pages = PageTestUtils.buildPage(runtime.getBufferAllocator(), schema, 1L, 32864L, Instant.ofEpochSecond(1422386629), Instant.ofEpochSecond(1422316800),  true, 123.45, "embulk");
        assertThat(pages.size(), is(1));
        for (Page page : pages) {
            output.add(page);
        }

        output.finish();
        output.commit();
        Thread.sleep(3000); // Need to wait until index done

        SearchResponse<IndexData> response = openSearchClient.search(s -> s.index(ES_INDEX), IndexData.class);

        int totalHits = response.hits().hits().size();

        assertThat(totalHits, is(1));

        IndexData record = response.hits().hits().get(0).source();
        assertThat(record.getId(), is(1L));
        assertThat(record.getAccount(), is(32864L));
        assertThat(record.getTime(), is("2015-01-27T19:23:49.000+0000"));
        assertThat(record.getPurchase(), is("2015-01-27T00:00:00.000+0000"));
        assertThat(record.getFlg(), is(true));
        assertThat(record.getScore(), is(123.45));
        assertThat(record.getComment(), is("embulk"));
    }

    @Test
    public void testOpenAbort()
    {
        ConfigSource config = utils.config();
        Schema schema = CONFIG_MAPPER.map(utils.oldParserConfig(runtime), CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        TransactionalPageOutput output = plugin.open(task.toTaskSource(), schema, 0);
        output.abort();
        // no error happens.
    }

    @Test
    public void testMode()
    {
        assertThat(Mode.values().length, is(2));
        assertThat(Mode.valueOf("INSERT"), is(Mode.INSERT));
    }

    @Test
    public void testAuthMethod()
    {
        assertThat(AuthMethod.values().length, is(2));
        assertThat(AuthMethod.valueOf("BASIC"), is(AuthMethod.BASIC));
    }

    @Test(expected = ConfigException.class)
    public void testModeThrowsConfigException()
    {
        Mode.fromString("non-exists-mode");
    }
}
