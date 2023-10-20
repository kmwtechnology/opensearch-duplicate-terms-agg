/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.aggregations.bucket;

import java.util.Arrays;
import java.util.List;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;


public class DuplicateTermAggregationPlugin extends Plugin implements SearchPlugin {
  @Override
  public List<AggregationSpec> getAggregations() {
    return Arrays.asList(
        new AggregationSpec(DuplicateTermAggregationBuilder.NAME,
            DuplicateTermAggregationBuilder::new,
            DuplicateTermAggregationBuilder.PARSER)
            .addResultReader(InternalDuplicateTermAggregation::new)
            .setAggregatorRegistrar(DuplicateTermAggregationBuilder::registerAggregators)
    );
  }
}
