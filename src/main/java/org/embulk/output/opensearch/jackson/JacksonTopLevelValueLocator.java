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

package org.embulk.output.opensearch.jackson;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JacksonTopLevelValueLocator extends JacksonValueLocator
{
    public JacksonTopLevelValueLocator(final String name)
    {
        this.name = name;
    }

    @Override
    public JsonNode seekValue(final ObjectNode record)
    {
        return record.get(this.name);
    }

    @Override
    public void placeValue(final ObjectNode record, final JsonNode value)
    {
        record.set(this.name, value);
    }

    private String name;
}
