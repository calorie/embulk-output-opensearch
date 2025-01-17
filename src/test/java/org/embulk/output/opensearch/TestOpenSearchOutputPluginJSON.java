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
import org.msgpack.value.ImmutableArrayValue;
import org.msgpack.value.ImmutableMapValue;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.SearchResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.embulk.output.opensearch.OpenSearchTestUtils.ES_INDEX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;
import static org.msgpack.value.ValueFactory.newArray;
import static org.msgpack.value.ValueFactory.newInteger;
import static org.msgpack.value.ValueFactory.newMap;
import static org.msgpack.value.ValueFactory.newString;

public class TestOpenSearchOutputPluginJSON
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
        final PluginTask task = CONFIG_MAPPER.map(utils.configJSON(), PluginTask.class);
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
        final PluginTask task = CONFIG_MAPPER.map(utils.configJSON(), PluginTask.class);
        assertThat(task.getIndex(), is(ES_INDEX));
    }

    @Test
    public void testTransaction()
    {
        ConfigSource config = utils.configJSON();
        Schema schema = utils.JSONSchema();
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
        ConfigSource config = utils.configJSON();
        Schema schema = utils.JSONSchema();
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
        ConfigSource config = utils.configJSON();
        Schema schema = utils.JSONSchema();
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        plugin.cleanup(task.toTaskSource(), schema, 0, Arrays.asList(CONFIG_MAPPER_FACTORY.newTaskReport()));
        // no error happens
    }

    @Test
    public void testOutputByOpen() throws Exception
    {
        ConfigSource config = utils.configJSON();
        Schema schema = utils.JSONSchema();
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        plugin.transaction(config, schema, 0, new OutputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(CONFIG_MAPPER_FACTORY.newTaskReport());
            }
        });
        TransactionalPageOutput output = plugin.open(task.toTaskSource(), schema, 0);

        ImmutableMapValue product = newMap(newString("id"), newInteger(1L), newString("name"), newString("product"));
        ImmutableArrayValue products = newArray(product);

        List<Page> pages = PageTestUtils.buildPage(runtime.getBufferAllocator(), schema, 1L, 32864L, "2015-01-27 19:23:49", "2015-01-27",  true, 123.45, "embulk", product, products);
        assertThat(pages.size(), is(1));
        for (Page page : pages) {
            output.add(page);
        }

        output.finish();
        output.commit();
        Thread.sleep(1500); // Need to wait until index done

        SearchResponse<IndexDataJson> response = openSearchClient.search(s -> s.index(ES_INDEX), IndexDataJson.class);

        int totalHits = response.hits().hits().size();

        assertThat(totalHits, is(1));

        IndexDataJson record = response.hits().hits().get(0).source();
        assertThat(record.getId(), is(1L));
        assertThat(record.getAccount(), is(32864L));
        assertThat(record.getTime(), is("2015-01-27 19:23:49"));
        assertThat(record.getPurchase(), is("2015-01-27"));
        assertThat(record.getFlg(), is(true));
        assertThat(record.getScore(), is(123.45));
        assertThat(record.getComment(), is("embulk"));
        assertThat(record.getProduct().getId(), is(1L));
        assertThat(record.getProduct().getName(), is("product"));
        assertThat(record.getProducts().get(0).getId(), is(1L));
        assertThat(record.getProducts().get(0).getName(), is("product"));
    }

    @Test
    public void testOutputByOpenWithNulls() throws Exception
    {
        ConfigSource config = utils.configJSON();
        Schema schema = utils.JSONSchema();
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        plugin.transaction(config, schema, 0, new OutputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(CONFIG_MAPPER_FACTORY.newTaskReport());
            }
        });
        TransactionalPageOutput output = plugin.open(task.toTaskSource(), schema, 0);

        ImmutableMapValue product = newMap(newString("id"), newInteger(1L), newString("name"), newString("product"));
        ImmutableArrayValue products = newArray(product);

        List<Page> pages = PageTestUtils.buildPage(runtime.getBufferAllocator(), schema, 2L, null, null, "2015-01-27",  true, 123.45, "embulk", product, products);
        assertThat(pages.size(), is(1));
        for (Page page : pages) {
            output.add(page);
        }

        output.finish();
        output.commit();
        Thread.sleep(1500); // Need to wait until index done

        SearchResponse<IndexDataJson> response = openSearchClient.search(s -> s.index(ES_INDEX), IndexDataJson.class);

        int totalHits = response.hits().hits().size();

        assertThat(totalHits, is(1));

        IndexDataJson record = response.hits().hits().get(0).source();
        assertThat(record.getId(), is(2L));
        assertTrue(record.getAccount() == null);
        assertTrue(record.getTime() == null);
        assertThat(record.getPurchase(), is("2015-01-27"));
        assertThat(record.getFlg(), is(true));
        assertThat(record.getScore(), is(123.45));
        assertThat(record.getComment(), is("embulk"));
        assertThat(record.getProduct().getId(), is(1L));
        assertThat(record.getProduct().getName(), is("product"));
        assertThat(record.getProducts().get(0).getId(), is(1L));
        assertThat(record.getProducts().get(0).getName(), is("product"));
    }

    @Test
    public void testOpenAbort()
    {
        ConfigSource config = utils.configJSON();
        Schema schema = utils.JSONSchema();
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
