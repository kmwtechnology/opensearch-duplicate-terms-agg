/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.aggregations.bucket.terms;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.hamcrest.Matchers;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE)
public class DuplicateTermsAggregationPluginIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(DuplicateTermsAggregationPlugin.class);
    }

    public void testPluginInstalled() throws IOException, ParseException {
        Response response = createRestClient().performRequest(new Request("GET", "/_cat/plugins"));
        String body = EntityUtils.toString(response.getEntity());

        logger.info("response body: {}", body);
        assertTrue(body.contains("rename"));
    }
}
