package org.opensearch.search.aggregations.bucket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.NonCollectingAggregator;
import org.opensearch.search.aggregations.metrics.MetricAggregatorSupplier;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.internal.SearchContext;

public class DuplicateTermAggregationFactory extends ValuesSourceAggregatorFactory {
  private long minDocCount;
  private BucketOrder order;
  private final DuplicateTermAggregator.BucketCountThresholds bucketCountThresholds;

  DuplicateTermAggregationFactory(String name,
      ValuesSourceConfig config,
      BucketOrder order,
      long minDocCount,
      DuplicateTermAggregator.BucketCountThresholds bucketCountThresholds,
      QueryShardContext context,
      AggregatorFactory parent,
      AggregatorFactories.Builder subFactoriesBuilder,
      Map<String, Object> metadata
  ) throws IOException {
    super(name, config, context, parent, subFactoriesBuilder, metadata);
    this.order = order;
    this.minDocCount = minDocCount;
    this.bucketCountThresholds = bucketCountThresholds;
  }

  public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
    builder.register(DuplicateTermAggregationBuilder.REGISTRY_KEY, CoreValuesSourceType.KEYWORD, (name,
            factories,
            order,
            roundingsInfo,
            minDocCount,
            bucketCountThresholds,
            valuesSourceConfig,
            aggregationContext,
            parent,
            cardinality,
            metadata) -> null,
        true);
  }

  @Override
  protected Aggregator createUnmapped(Aggregator parent,
      Map<String, Object> metadata) throws IOException {
    final InternalAggregation aggregation = new InternalDuplicateTermAggregation(name, new ArrayList<>(), order, minDocCount,
        bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getShardSize(), 0, metadata);
    return new NonCollectingAggregator(name, context, parent, factories, metadata) {
      {
        // even in the case of an unmapped aggregator, validate the
        // order
        order.validate(this);
      }

      @Override
      public InternalAggregation buildEmptyAggregation() { return aggregation; }
    };
  }

  @Override
  protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata
  ) throws IOException {

    DuplicateTermAggregator.BucketCountThresholds bucketCountThresholds = new
        DuplicateTermAggregator.BucketCountThresholds(this.bucketCountThresholds);
    if (!InternalOrder.isKeyOrder(order)
        && bucketCountThresholds.getShardSize() == DuplicateTermAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize()) {
      // The user has not made a shardSize selection. Use default
      // heuristic to avoid any wrong-ranking caused by distributed
      // counting
      bucketCountThresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(bucketCountThresholds.getRequiredSize()));
    }
    bucketCountThresholds.ensureValidity();
    return new DuplicateTermAggregator(
        name, factories, context, (ValuesSource.Keyword) config.getValuesSource(),
        order, minDocCount, bucketCountThresholds, roundingsInfo, parent, cardinality, metadata);
  }
}
