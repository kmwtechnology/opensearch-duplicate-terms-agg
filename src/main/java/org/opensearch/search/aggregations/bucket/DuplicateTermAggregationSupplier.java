/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.aggregations.bucket;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.util.BytesRef;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

public interface DuplicateTermAggregationSupplier {
  Aggregator build(String name,
      AggregatorFactories factories,
      BytesRef separator,
      int minDepth,
      int maxDepth,
      boolean keepBlankPath,
      BucketOrder order,
      long minDocCount,
      DuplicateTermAggregator.BucketCountThresholds bucketCountThresholds,
      ValuesSourceConfig valuesSourceConfig,
      SearchContext aggregationContext,
      Aggregator parent,
      CardinalityUpperBound cardinality,
      Map<String, Object> metadata
  ) throws IOException;

}
