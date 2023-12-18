/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.bucket.terms;

import static java.util.Collections.emptyList;

import java.io.IOException;
import java.util.Map;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.internal.SearchContext;

/**
 * Base Aggregator to collect all docs that contain significant terms
 *
 * @opensearch.internal
 */
abstract class DuplicateAbstractStringTermsAggregator extends TermsAggregator {

  protected final boolean showTermDocCountError;

  DuplicateAbstractStringTermsAggregator(
      String name,
      AggregatorFactories factories,
      SearchContext context,
      Aggregator parent,
      BucketOrder order,
      DocValueFormat format,
      BucketCountThresholds bucketCountThresholds,
      SubAggCollectionMode subAggCollectMode,
      boolean showTermDocCountError,
      Map<String, Object> metadata
  ) throws IOException {
    super(name, factories, context, parent, bucketCountThresholds, order, format, subAggCollectMode, metadata);
    this.showTermDocCountError = showTermDocCountError;
  }

  /**
   * Build empty Internal terms class
   */
  protected InternalDuplicateTerms buildEmptyTermsAggregation() {
    return new InternalDuplicateTerms(
        name,
        order,
        order,
        metadata(),
        format,
        bucketCountThresholds.getShardSize(),
        showTermDocCountError,
        0,
        emptyList(),
        0,
        bucketCountThresholds
    );
  }
}
