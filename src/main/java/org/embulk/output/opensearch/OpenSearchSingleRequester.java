/*
 * Copyright 2020 The Embulk project
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

public abstract class OpenSearchSingleRequester<T>
{
    public abstract T requestOnce(org.opensearch.client.opensearch.OpenSearchClient client);

    public final boolean toRetry(final Exception exception)
    {
        if (exception instanceof org.opensearch.client.opensearch._types.OpenSearchException) {
            return isResponseStatusToRetry(((org.opensearch.client.opensearch._types.OpenSearchException) exception).response());
        }
        else {
            return isExceptionToRetry(exception);
        }
    }

    protected boolean isResponseStatusToRetry(final org.opensearch.client.opensearch._types.ErrorResponse response)
    {
        int status = response.status();
        if (status == 404) {
            throw new ResourceNotFoundException("Requested resource was not found");
        }
        else if (status == 429) {
            return true; // Retry if 429.
        }
        return status / 100 != 4; // Retry unless 4xx except for 429.
    }

    protected boolean isExceptionToRetry(final Exception exception)
    {
        return false;
    }
}
