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

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationExecutionException;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.NonCollectingAggregator;
import org.opensearch.search.aggregations.bucket.BucketUtils;
import org.opensearch.search.aggregations.bucket.terms.DuplicateTermsAggregator.ValuesSourceCollectorSource;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.internal.SearchContext;

/**
 * Aggregation Factory for terms agg
 *
 * @opensearch.internal
 */
public class DuplicateTermsAggregatorFactory extends ValuesSourceAggregatorFactory {

  // Registers aggregator via the Key, ValuesSourceTypes, and supplier
  static void registerAggregators(ValuesSourceRegistry.Builder builder) {
    // This plug-in does not support numeric types, therefore no numericSupplier
    builder.register(
        DuplicateTermsAggregationBuilder.REGISTRY_KEY,
        CoreValuesSourceType.BYTES,
        DuplicateTermsAggregatorFactory.supplier(),
        true
    );
  }

  /**
   * This supplier is used for all the field types that should be aggregated as bytes/strings,
   * including those that need global ordinals
   */
  private static DuplicateTermsAggregatorSupplier supplier() {
    return new DuplicateTermsAggregatorSupplier() {
      @Override
      public Aggregator build(
          String separator,
          String aggField,
          String name,
          AggregatorFactories factories,
          ValuesSource valuesSource,
          BucketOrder order,
          DocValueFormat format,
          TermsAggregator.BucketCountThresholds bucketCountThresholds,
          IncludeExclude includeExclude,
          String executionHint,
          SearchContext context,
          Aggregator parent,
          SubAggCollectionMode subAggCollectMode,
          boolean showTermDocCountError,
          CardinalityUpperBound cardinality,
          Map<String, Object> metadata
      ) throws IOException {

        if (subAggCollectMode == null) {
          subAggCollectMode = SubAggCollectionMode.DEPTH_FIRST; // we default to DEPTH_FIRST as this is how MapStringTermsAggregator defaults
        }

        // IncludeExclude defines the regex filtering
        if ((includeExclude != null) && (includeExclude.isRegexBased()) && format != DocValueFormat.RAW) {
          // TODO this exception message is not really accurate for the string case. It's really disallowing regex + formatter
          throw new AggregationExecutionException(
              "Aggregation ["
                  + name
                  + "] cannot support regular expression style "
                  + "include/exclude settings as they can only be applied to string fields. Use an array of values for "
                  + "include/exclude clauses"
          );
        }

        int maxRegexLength = context.getQueryShardContext().getIndexSettings().getMaxRegexLength();
        final IncludeExclude.StringFilter filter = includeExclude == null
            ? null
            : includeExclude.convertToStringFilter(format, maxRegexLength);

        return new DuplicateTermsAggregator(
            separator,
            aggField,
            name,
            factories,
            new ValuesSourceCollectorSource(valuesSource, separator, aggField),
            a -> a.new StandardTermsResults(valuesSource),
            order,
            format,
            bucketCountThresholds,
            filter,
            context,
            parent,
            subAggCollectMode,
            showTermDocCountError,
            cardinality,
            metadata
        );
      }
    };
  }

  private final String separator;
  private final String aggField;
  private final BucketOrder order;
  private final IncludeExclude includeExclude;
  private final String executionHint;
  private final SubAggCollectionMode collectMode;
  private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
  private final boolean showTermDocCountError;

  DuplicateTermsAggregatorFactory(
      String separator,
      String aggField,
      String name,
      ValuesSourceConfig config,
      BucketOrder order,
      IncludeExclude includeExclude,
      String executionHint,
      SubAggCollectionMode collectMode,
      TermsAggregator.BucketCountThresholds bucketCountThresholds,
      boolean showTermDocCountError,
      QueryShardContext queryShardContext,
      AggregatorFactory parent,
      AggregatorFactories.Builder subFactoriesBuilder,
      Map<String, Object> metadata
  ) throws IOException {
    super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
    this.separator = separator;
    this.aggField = aggField;
    this.order = order;
    this.includeExclude = includeExclude;
    this.executionHint = executionHint;
    this.collectMode = collectMode;
    this.bucketCountThresholds = bucketCountThresholds;
    this.showTermDocCountError = showTermDocCountError;
  }

  // Only called if the config has no values in doCreateInternal() of ValuesSourceAggregatoryFactory.java
  @Override
  protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
    final InternalAggregation aggregation = new UnmappedTerms(name, order, bucketCountThresholds, metadata);
    Aggregator agg = new NonCollectingAggregator(name, searchContext, parent, factories, metadata) {
      @Override
      public InternalAggregation buildEmptyAggregation() {
        return aggregation;
      }
    };
    // even in the case of an unmapped aggregator, validate the order
    order.validate(agg);
    return agg;
  }

  // Creates the aggregation object
  @Override
  protected Aggregator doCreateInternal(
      SearchContext searchContext,
      Aggregator parent,
      CardinalityUpperBound cardinality,
      Map<String, Object> metadata
  ) throws IOException {
    DuplicateTermsAggregatorSupplier aggregatorSupplier = queryShardContext.getValuesSourceRegistry()
        .getAggregator(DuplicateTermsAggregationBuilder.REGISTRY_KEY, config);
    BucketCountThresholds bucketCountThresholds = new BucketCountThresholds(this.bucketCountThresholds);
    if (InternalOrder.isKeyOrder(order) == false
        && bucketCountThresholds.getShardSize() == DuplicateTermsAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize()) {
      // The user has not made a shardSize selection. Use default
      // heuristic to avoid any wrong-ranking caused by distributed
      // counting
      bucketCountThresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(bucketCountThresholds.getRequiredSize()));
    }
    bucketCountThresholds.ensureValidity();

    return aggregatorSupplier.build(
        separator,
        aggField,
        name,
        factories,
        config.getValuesSource(),
        order,
        config.format(),
        bucketCountThresholds,
        includeExclude,
        executionHint,
        searchContext,
        parent,
        collectMode,
        showTermDocCountError,
        cardinality,
        metadata
    );
  }
}
