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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.InternalOrder.CompoundOrder;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.aggregations.support.ValuesSourceType;

/**
 * Aggregation Builder for terms agg
 *
 * @opensearch.internal
 */
public class DuplicateTermsAggregationBuilder extends ValuesSourceAggregationBuilder<DuplicateTermsAggregationBuilder> {

  // Name of aggregation; name that is called when performing aggregation
  public static final String NAME = "duplicate_terms";

  // Registry key for ValuesSourceRegistry; maps types to functions that build aggregations
  public static final ValuesSourceRegistry.RegistryKey<DuplicateTermsAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
      NAME,
      DuplicateTermsAggregatorSupplier.class
  );

  // execution hint (not relevant for this plug-in, kept in case of future development)
  public static final ParseField EXECUTION_HINT_FIELD_NAME = new ParseField("execution_hint");
  public static final ParseField SHARD_SIZE_FIELD_NAME = new ParseField("shard_size");
  public static final ParseField MIN_DOC_COUNT_FIELD_NAME = new ParseField("min_doc_count");
  public static final ParseField SHARD_MIN_DOC_COUNT_FIELD_NAME = new ParseField("shard_min_doc_count");
  public static final ParseField REQUIRED_SIZE_FIELD_NAME = new ParseField("size");
  // field name on which aggregation must occur
  public static final ParseField AGGREGATION_FIELD = new ParseField("agg_field");
  // separator by which values and value counts will be "separated" in the aggregation
  public static final ParseField SEPARATOR_FIELD = new ParseField("separator");

  // Defaults for bucket counts
  public static final TermsAggregator.BucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS = new TermsAggregator.BucketCountThresholds(
      1,
      0,
      10,
      -1
  );

  public static final ParseField SHOW_TERM_DOC_COUNT_ERROR = new ParseField("show_term_doc_count_error");
  public static final ParseField ORDER_FIELD = new ParseField("order");
  // Following code is related to how the information is parsed from an aggregation request
  public static final ObjectParser<DuplicateTermsAggregationBuilder, String> PARSER =
      ObjectParser.fromBuilder(NAME, DuplicateTermsAggregationBuilder::new);

  static {
    ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);

    PARSER.declareString(DuplicateTermsAggregationBuilder::aggField, AGGREGATION_FIELD);

    PARSER.declareString(DuplicateTermsAggregationBuilder::separator, SEPARATOR_FIELD);

    PARSER.declareBoolean(DuplicateTermsAggregationBuilder::showTermDocCountError, DuplicateTermsAggregationBuilder.SHOW_TERM_DOC_COUNT_ERROR);

    PARSER.declareInt(DuplicateTermsAggregationBuilder::shardSize, SHARD_SIZE_FIELD_NAME);

    PARSER.declareLong(DuplicateTermsAggregationBuilder::minDocCount, MIN_DOC_COUNT_FIELD_NAME);

    PARSER.declareLong(DuplicateTermsAggregationBuilder::shardMinDocCount, SHARD_MIN_DOC_COUNT_FIELD_NAME);

    PARSER.declareInt(DuplicateTermsAggregationBuilder::size, REQUIRED_SIZE_FIELD_NAME);

    PARSER.declareString(DuplicateTermsAggregationBuilder::executionHint, EXECUTION_HINT_FIELD_NAME);

    PARSER.declareField(
        DuplicateTermsAggregationBuilder::collectMode,
        (p, c) -> SubAggCollectionMode.parse(p.text(), LoggingDeprecationHandler.INSTANCE),
        SubAggCollectionMode.KEY,
        ObjectParser.ValueType.STRING
    );

    PARSER.declareObjectArray(
        DuplicateTermsAggregationBuilder::order,
        (p, c) -> InternalOrder.Parser.parseOrderParam(p),
        TermsAggregationBuilder.ORDER_FIELD
    );

    PARSER.declareField(
        (b, v) -> b.includeExclude(IncludeExclude.merge(v, b.includeExclude())),
        IncludeExclude::parseInclude,
        IncludeExclude.INCLUDE_FIELD,
        ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING
    );

    PARSER.declareField(
        (b, v) -> b.includeExclude(IncludeExclude.merge(b.includeExclude(), v)),
        IncludeExclude::parseExclude,
        IncludeExclude.EXCLUDE_FIELD,
        ObjectParser.ValueType.STRING_ARRAY
    );
  }

  public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
    return PARSER.parse(parser, new DuplicateTermsAggregationBuilder(aggregationName), null);
  }

  public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
    DuplicateTermsAggregatorFactory.registerAggregators(builder);
  }

  private static final String DEFAULT_SEPARATOR = "_";

  private static final String DEFAULT_AGG_FIELD = "favorite_foods";

  private String aggField = DEFAULT_AGG_FIELD;
  private String separator = DEFAULT_SEPARATOR;
  private BucketOrder order = BucketOrder.compound(BucketOrder.count(false)); // automatically adds tie-breaker key asc order
  private IncludeExclude includeExclude = null;
  private String executionHint = null;
  private SubAggCollectionMode collectMode = null;
  private DuplicateTermsAggregator.BucketCountThresholds bucketCountThresholds = DEFAULT_BUCKET_COUNT_THRESHOLDS;
  private boolean showTermDocCountError = false;


  private DuplicateTermsAggregationBuilder(String name) {
    super(name);
  }

  protected DuplicateTermsAggregationBuilder(
      DuplicateTermsAggregationBuilder clone,
      AggregatorFactories.Builder factoriesBuilder,
      Map<String, Object> metadata
  ) {
    super(clone, factoriesBuilder, metadata);
    this.separator = clone.separator;
    this.aggField = clone.aggField;
    this.order = clone.order;
    this.executionHint = clone.executionHint;
    this.includeExclude = clone.includeExclude;
    this.collectMode = clone.collectMode;
    this.bucketCountThresholds = new DuplicateTermsAggregator.BucketCountThresholds(clone.bucketCountThresholds);
    this.showTermDocCountError = clone.showTermDocCountError;
  }

  // Keywords are represented as the BYTES type in OS, ES currently represents them as KEYWORD
  @Override
  protected ValuesSourceType defaultValueSourceType() {
    return CoreValuesSourceType.BYTES;
  }

  @Override
  protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
    return new DuplicateTermsAggregationBuilder(this, factoriesBuilder, metadata);
  }

  // Uncomment for logging
  //  private static final Logger log = LogManager.getLogger(DuplicateTermsAggregationBuilder.class);

  /**
   * Read from a stream.
   */
  public DuplicateTermsAggregationBuilder(StreamInput in) throws IOException {
    super(in);
    separator = in.readString();
    aggField = in.readString();
    bucketCountThresholds = new DuplicateTermsAggregator.BucketCountThresholds(in);
    //collectMode = in.readOptionalWriteable(SubAggCollectionMode::readFromStream);
    //executionHint = in.readOptionalString();
    //includeExclude = in.readOptionalWriteable(IncludeExclude::new);
    //order = InternalOrder.Streams.readOrder(in);
    //showTermDocCountError = in.readBoolean();
  }

  @Override
  protected boolean serializeTargetValueType(Version version) {
    return true;
  }

  @Override
  protected void innerWriteTo(StreamOutput out) throws IOException {
    bucketCountThresholds.writeTo(out);
    out.writeOptionalString(separator);
    out.writeString(aggField);
    out.writeOptionalWriteable(collectMode);
    out.writeOptionalString(executionHint);
    out.writeOptionalWriteable(includeExclude);
    order.writeTo(out);
    out.writeBoolean(showTermDocCountError);
  }

  /**
   * Set aggField
   */
  public DuplicateTermsAggregationBuilder aggField(String aggField) {
    this.aggField = aggField;
    return this;
  }

  /**
   * Set separator by which we separate the counts
   */
  public DuplicateTermsAggregationBuilder separator(String separator) {
    this.separator = separator;
    return this;
  }

  /**
   * Sets the size - indicating how many term buckets should be returned
   * (defaults to 10)
   */
  public DuplicateTermsAggregationBuilder size(int size) {
    if (size <= 0) {
      throw new IllegalArgumentException("[size] must be greater than 0. Found [" + size + "] in [" + name + "]");
    }
    bucketCountThresholds.setRequiredSize(size);
    return this;
  }

  /**
   * Returns the number of term buckets currently configured
   */
  public int size() {
    return bucketCountThresholds.getRequiredSize();
  }

  /**
   * Sets the shard_size - indicating the number of term buckets each shard
   * will return to the coordinating node (the node that coordinates the
   * search execution). The higher the shard size is, the more accurate the
   * results are.
   */
  public DuplicateTermsAggregationBuilder shardSize(int shardSize) {
    if (shardSize <= 0) {
      throw new IllegalArgumentException("[shardSize] must be greater than 0. Found [" + shardSize + "] in [" + name + "]");
    }
    bucketCountThresholds.setShardSize(shardSize);
    return this;
  }

  /**
   * Returns the number of term buckets per shard that are currently configured
   */
  public int shardSize() {
    return bucketCountThresholds.getShardSize();
  }

  /**
   * Set the minimum document count terms should have in order to appear in
   * the response.
   */
  public DuplicateTermsAggregationBuilder minDocCount(long minDocCount) {
    if (minDocCount < 0) {
      throw new IllegalArgumentException(
          "[minDocCount] must be greater than or equal to 0. Found [" + minDocCount + "] in [" + name + "]"
      );
    }
    bucketCountThresholds.setMinDocCount(minDocCount);
    return this;
  }

  /**
   * Returns the minimum document count required per term
   */
  public long minDocCount() {
    return bucketCountThresholds.getMinDocCount();
  }


  /**
   * Set the minimum document count terms should have on the shard in order to
   * appear in the response.
   */
  public DuplicateTermsAggregationBuilder shardMinDocCount(long shardMinDocCount) {
    if (shardMinDocCount < 0) {
      throw new IllegalArgumentException(
          "[shardMinDocCount] must be greater than or equal to 0. Found [" + shardMinDocCount + "] in [" + name + "]"
      );
    }
    bucketCountThresholds.setShardMinDocCount(shardMinDocCount);
    return this;
  }

  /**
   * Returns the minimum document count required per term, per shard
   */
  public long shardMinDocCount() {
    return bucketCountThresholds.getShardMinDocCount();
  }

  /** Set a new order on this builder and return the builder so that calls
   *  can be chained. A tie-breaker may be added to avoid non-deterministic ordering. */
  public DuplicateTermsAggregationBuilder order(BucketOrder order) {
    if (order == null) {
      throw new IllegalArgumentException("[order] must not be null: [" + name + "]");
    }
    if (order instanceof CompoundOrder || InternalOrder.isKeyOrder(order)) {
      this.order = order; // if order already contains a tie-breaker we are good to go
    } else { // otherwise add a tie-breaker by using a compound order
      this.order = BucketOrder.compound(order);
    }
    return this;
  }

  /**
   * Sets the order in which the buckets will be returned. A tie-breaker may be added to avoid non-deterministic
   * ordering.
   */
  public DuplicateTermsAggregationBuilder order(List<BucketOrder> orders) {
    if (orders == null) {
      throw new IllegalArgumentException("[orders] must not be null: [" + name + "]");
    }
    // if the list only contains one order use that to avoid inconsistent xcontent
    order(orders.size() > 1 ? BucketOrder.compound(orders) : orders.get(0));
    return this;
  }

  /**
   * Gets the order in which the buckets will be returned.
   */
  public BucketOrder order() {
    return order;
  }

  /**
   * Expert: sets an execution hint to the aggregation.
   */
  public DuplicateTermsAggregationBuilder executionHint(String executionHint) {
    this.executionHint = executionHint;
    return this;
  }

  /**
   * Expert: gets an execution hint to the aggregation.
   */
  public String executionHint() {
    return executionHint;
  }

  /**
   * Expert: set the collection mode.
   */
  public DuplicateTermsAggregationBuilder collectMode(SubAggCollectionMode collectMode) {
    if (collectMode == null) {
      throw new IllegalArgumentException("[collectMode] must not be null: [" + name + "]");
    }
    this.collectMode = collectMode;
    return this;
  }

  /**
   * Expert: get the collection mode.
   */
  public SubAggCollectionMode collectMode() {
    return collectMode;
  }

  /**
   * Set terms to include and exclude from the aggregation results
   */
  public DuplicateTermsAggregationBuilder includeExclude(IncludeExclude includeExclude) {
    this.includeExclude = includeExclude;
    return this;
  }

  /**
   * Get terms to include and exclude from the aggregation results
   */
  public IncludeExclude includeExclude() {
    return includeExclude;
  }

  /**
   * Get whether doc count error will be return for individual terms
   */
  public boolean showTermDocCountError() {
    return showTermDocCountError;
  }

  /**
   * Set whether doc count error will be return for individual terms
   */
  public DuplicateTermsAggregationBuilder showTermDocCountError(boolean showTermDocCountError) {
    this.showTermDocCountError = showTermDocCountError;
    return this;
  }

  @Override
  public BucketCardinality bucketCardinality() {
    return BucketCardinality.MANY;
  }

  @Override
  protected ValuesSourceAggregatorFactory innerBuild(
      QueryShardContext queryShardContext,
      ValuesSourceConfig config,
      AggregatorFactory parent,
      AggregatorFactories.Builder subFactoriesBuilder
  ) throws IOException {
    return new DuplicateTermsAggregatorFactory(
        separator,
        aggField,
        name,
        config,
        order,
        includeExclude,
        executionHint,
        collectMode,
        bucketCountThresholds,
        showTermDocCountError,
        queryShardContext,
        parent,
        subFactoriesBuilder,
        metadata
    );
  }

  @Override
  protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
    bucketCountThresholds.toXContent(builder, params);
    builder.field(SHOW_TERM_DOC_COUNT_ERROR.getPreferredName(), showTermDocCountError);
    builder.field(AGGREGATION_FIELD.getPreferredName(), aggField);
    if (executionHint != null) {
      builder.field(DuplicateTermsAggregationBuilder.EXECUTION_HINT_FIELD_NAME.getPreferredName(), executionHint);
    }
    builder.field(ORDER_FIELD.getPreferredName());
    order.toXContent(builder, params);
    if (collectMode != null) {
      builder.field(SubAggCollectionMode.KEY.getPreferredName(), collectMode.parseField().getPreferredName());
    }
    if (includeExclude != null) {
      includeExclude.toXContent(builder, params);
    }
    if (separator != null) {
      builder.field(DuplicateTermsAggregationBuilder.SEPARATOR_FIELD.getPreferredName(), separator);
    }
    return builder;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        bucketCountThresholds,
        collectMode,
        executionHint,
        includeExclude,
        order,
        showTermDocCountError
    );
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    if (super.equals(obj) == false) return false;
    DuplicateTermsAggregationBuilder other = (DuplicateTermsAggregationBuilder) obj;
    return Objects.equals(bucketCountThresholds, other.bucketCountThresholds)
        && Objects.equals(collectMode, other.collectMode)
        && Objects.equals(executionHint, other.executionHint)
        && Objects.equals(includeExclude, other.includeExclude)
        && Objects.equals(order, other.order)
        && Objects.equals(showTermDocCountError, other.showTermDocCountError);
  }

  @Override
  public String getType() {
    return NAME;
  }

  @Override
  protected AggregationBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
    return super.doRewrite(queryShardContext);
  }

  @Override
  protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
    return REGISTRY_KEY;
  }

}
