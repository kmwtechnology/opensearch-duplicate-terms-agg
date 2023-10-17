/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.aggregations.bucket;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.logging.log4j.core.util.IOUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.plugin.UniqueCountAggregationPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE)
public class UniqueCountAggregationPluginIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(UniqueCountAggregationPlugin.class);
    }

    public void testPluginInstalled() throws IOException, ParseException {
        Response response = createRestClient().performRequest(new Request("GET", "/_cat/plugins"));
        //String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        InputStream inputStream = response.getEntity().getContent();
        String body = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);

        logger.info("response body: {}", body);
        assertThat(body, containsString("UniqueCountAggregation"));
    }
}
