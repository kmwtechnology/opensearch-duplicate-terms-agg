package org.opensearch.search.aggregations.bucket;

import java.io.IOException;
import java.util.Map;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry.RegistryKey;
import org.opensearch.search.aggregations.support.ValuesSourceType;

public class UniqueCountAggregationBuilder extends ValuesSourceAggregationBuilder<UniqueCountAggregationBuilder> {
  public static final String NAME = "unique_count";
  public static final ValuesSourceRegistry.RegistryKey<UniqueCountAggregationSupplier> REGISTRY_KEY =
      new ValuesSourceRegistry.RegistryKey<>(NAME, UniqueCountAggregationSupplier.class);

  public static final ObjectParser<UniqueCountAggregationBuilder, String> PARSER =
      ObjectParser.fromBuilder(NAME, UniqueCountAggregationBuilder::new);
  static {
    ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
  }

  public UniqueCountAggregationBuilder(String name) {
    super(name);
  }

  public UniqueCountAggregationBuilder(UniqueCountAggregationBuilder clone, AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
    super(clone, factoriesBuilder, metadata);
    // this.property = clone.property;
    // if any config properties are added, you need to have the clone's properties set here as shown above
  }

  public UniqueCountAggregationBuilder(StreamInput in) throws IOException {
    super(in);
    // this.property = in.readDouble();
    // if any config properties are added, you need to read them from input stream here as shown above
  }


  @Override
  protected void innerWriteTo(StreamOutput out) throws IOException {

  }

  @Override
  protected RegistryKey<?> getRegistryKey() {
    return null;
  }

  @Override
  protected ValuesSourceType defaultValueSourceType() {
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
  protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
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
