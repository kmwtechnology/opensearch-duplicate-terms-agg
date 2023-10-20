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
import org.opensearch.Version;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry.RegistryKey;
import org.opensearch.search.aggregations.support.ValuesSourceType;

public class DuplicateTermAggregationBuilder extends ValuesSourceAggregationBuilder<DuplicateTermAggregationBuilder> {
  public static final String NAME = "duplicate_count";
  public static final ValuesSourceRegistry.RegistryKey<DuplicateTermAggregationSupplier> REGISTRY_KEY =
      new ValuesSourceRegistry.RegistryKey<>(NAME, DuplicateTermAggregationSupplier.class);

  public static final DuplicateTermAggregator.BucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS = new
      DuplicateTermAggregator.BucketCountThresholds(10, -1);
  public static final ObjectParser<DuplicateTermAggregationBuilder, String> PARSER =
      ObjectParser.fromBuilder(NAME, DuplicateTermAggregationBuilder::new);
  static {
    ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
  }

  public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
    return PARSER.parse(parser, new DuplicateTermAggregationBuilder(aggregationName), null);
  }

  public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
    DuplicateTermAggregationFactory.registerAggregators(builder);
  }

  private DuplicateTermAggregator.BucketCountThresholds bucketCountThresholds = new DuplicateTermAggregator(
      DEFAULT_BUCKET_COUNT_THRESHOLDS);
  // instantiate default values for fields here

  public DuplicateTermAggregationBuilder(String name) {
    super(name);
  }

  @Override
  protected boolean serializeTargetValueType(Version version) {
    return true;
  }

  public DuplicateTermAggregationBuilder(StreamInput in) throws IOException {
    super(in);
    // this.property = in.readDouble();
    // if any config properties are added, you need to read them from input stream here as shown above
  }

  public DuplicateTermAggregationBuilder(DuplicateTermAggregationBuilder clone, AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
    super(clone, factoriesBuilder, metadata);
    // this.property = clone.property;
    // if any config properties are added, you need to have the clone's properties set here as shown above
  }

  @Override
  protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
    return new DuplicateTermAggregationBuilder(this, factoriesBuilder, metadata);
  }

  @Override
  protected ValuesSourceType defaultValueSourceType() {
    return CoreValuesSourceType.KEYWORD; // cannot find keyword type in 2.X implementation of OS, need to find alternative
  }


  @Override
  protected void innerWriteTo(StreamOutput out) throws IOException {
    bucketCountThresholds.writeTo(out);
  }

  @Override
  protected RegistryKey<?> getRegistryKey() {
    return null;
  }

  @Override
  protected ValuesSourceAggregatorFactory innerBuild(QueryShardContext queryShardContext, ValuesSourceConfig config,
      AggregatorFactory parent, Builder subFactoriesBuilder) throws IOException {
    return null;
  }

  @Override
  protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
    return null;
  }



  @Override
  public BucketCardinality bucketCardinality() {
    return null;
  }

  @Override
  public String getType() {
    return null;
  }
}
