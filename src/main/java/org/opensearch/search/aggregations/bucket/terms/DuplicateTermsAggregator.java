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

import static org.opensearch.search.aggregations.InternalOrder.isKeyOrder;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.LocalBucketCountThresholds;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

public class DuplicateTermsAggregator extends DuplicateAbstractStringTermsAggregator {

  // Uncomment if you'd like to have logging
  // private static final Logger log = LogManager.getLogger(DuplicateTermsAggregator.class);

  private final String separator;
  private final String aggField;
  private final DuplicateTermsAggregator.CollectorSource collectorSource;
  private final StandardTermsResults standardTermsResults;
  private final BytesKeyedBucketOrds bucketOrds;
  private final IncludeExclude.StringFilter includeExclude;

  private final BucketCountThresholds duplicateBucketCountThresholds;
  private final Comparator<InternalDuplicateTerms.InternalBucket> duplicatePartiallyBuiltBucketComparator;
  private final DocValueFormat duplicateFormat;
  private final BucketOrder duplicateOrder;

  public DuplicateTermsAggregator(
      String separator,
      String aggField,
      String name,
      AggregatorFactories factories,
      DuplicateTermsAggregator.CollectorSource collectorSource,
      Function<DuplicateTermsAggregator, StandardTermsResults> strategy,
      BucketOrder order,
      DocValueFormat format,
      BucketCountThresholds bucketCountThresholds,
      IncludeExclude.StringFilter includeExclude,
      SearchContext context,
      Aggregator parent,
      SubAggCollectionMode collectionMode,
      boolean showTermDocCountError,
      CardinalityUpperBound cardinality,
      Map<String, Object> metadata
  ) throws IOException {
    super(name, factories, context, parent, order, format, bucketCountThresholds, collectionMode, showTermDocCountError, metadata);
    this.separator = separator;
    this.aggField = aggField;
    this.collectorSource = collectorSource;
    this.standardTermsResults = strategy.apply(this); // ResultStrategy needs a reference to the Aggregator to do its job.
    this.includeExclude = includeExclude;
    bucketOrds = BytesKeyedBucketOrds.build(context.bigArrays(), cardinality);
    this.duplicateBucketCountThresholds = bucketCountThresholds;
    duplicatePartiallyBuiltBucketComparator = order == null ? null : order.partiallyBuiltBucketComparator(b -> b.bucketOrd, this);
    duplicateFormat = format;
    duplicateOrder = order;
  }

  @Override
  public ScoreMode scoreMode() {
    if (collectorSource.needsScores()) {
      return ScoreMode.COMPLETE;
    }
    return super.scoreMode();
  }

  @Override
  public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
    return standardTermsResults.wrapCollector(
        collectorSource.getLeafCollector(
            includeExclude,
            ctx,
            sub,
            this::addRequestCircuitBreakerBytes,
            (s, doc, owningBucketOrd, bytes) -> {
              long bucketOrdinal = bucketOrds.add(owningBucketOrd, bytes);
              if (bucketOrdinal < 0) { // already seen
                bucketOrdinal = -1 - bucketOrdinal;
                collectExistingBucket(s, doc, bucketOrdinal);
              } else {
                collectBucket(s, doc, bucketOrdinal);
              }
            }
        )
    );
  }

  @Override
  public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
    return standardTermsResults.buildAggregations(owningBucketOrds);
  }

  @Override
  public InternalAggregation buildEmptyAggregation() {
    return standardTermsResults.buildEmptyResult();
  }

  @Override
  public void collectDebugInfo(BiConsumer<String, Object> add) {
    super.collectDebugInfo(add);
    add.accept("total_buckets", bucketOrds.size());
    add.accept("result_strategy", standardTermsResults.describe());
  }

  @Override
  public void doClose() {
    Releasables.close(collectorSource, bucketOrds);
  }

  // Duplicate terms aggregation logic occurs here
  public static SortedBinaryDocValues toString(final SortedSetDocValues values, String separator) {
    return new SortedBinaryDocValues() {
      private int count = 0;
      private BytesRef currentValue = null;
      private int remainingInstances = 0;

      private HashMap<Long, SimpleImmutableEntry<Integer, String>> ordsToSuffixMap = new HashMap<>();

      @Override
      public boolean advanceExact(int doc) throws IOException {
        ordsToSuffixMap.clear();
        if (values.advanceExact(doc) == false) {
          return false;
        } // ^This checks to make sure that the doc at the specified number has a value
        // foo_3, bar_1, baz_2 -> count = 6

        for (int i = 0;; ++i) {
          long currentOrd = values.nextOrd();
          if (currentOrd == SortedSetDocValues.NO_MORE_ORDS) {
            break;
          }
          SimpleImmutableEntry<Integer, String> pair = getSuffix(currentOrd);
          Integer suffix = pair.getKey();
          count = count + suffix; // we do this here because we want to be looking at each unique value, not just the final
          ordsToSuffixMap.put(currentOrd, pair); // we cache the pair into a hashmap for future retrieval, nets us performance gain
        }
        // reset the iterator on the current doc
        boolean advanced = values.advanceExact(doc);
        assert advanced;
        return true;
      }

      @Override
      public int docValueCount() {
        int oldCount = count;
        count = 0; // we reset the count here
        return oldCount;

      }

      // Separates the values based on provided separator and returns a Tuple of (count, value_without_count)
      public SimpleImmutableEntry<Integer, String> getSuffix(long ord) throws IOException {
        String value = values.lookupOrd(ord).utf8ToString();
        int suffixCount = 0;
        String withoutUnderscore = value;
        int lastIndexUnderscore = value.lastIndexOf(separator);
        if (lastIndexUnderscore != -1) {
          suffixCount = Integer.valueOf(value.substring(lastIndexUnderscore + 1, value.length()));
          withoutUnderscore = value.substring(0, lastIndexUnderscore);
        }
        return new AbstractMap.SimpleImmutableEntry<Integer, String>(suffixCount, withoutUnderscore);
      }

      @Override
      public BytesRef nextValue() throws IOException {
        // foo_3, bar_1, baz_2

        // two cases
        // 1. actually calling nextOrd for the next bucket
        // 2. calling a "fake" value which represents one of the suffix items that doesn't technically exist,
        //    but we need to account for
        if (currentValue != null && remainingInstances > 0) {
          remainingInstances -= 1;
          return currentValue;
        }

        remainingInstances = 0;
        long nextOrd = values.nextOrd();
        // If there are no more ords, we then go to the next "real" value
        if (nextOrd == SortedSetDocValues.NO_MORE_ORDS) {
          currentValue = null;
          return values.lookupOrd(nextOrd);
        }
        // else, we lower the count of "fake" values (or remainingInstances) and return the value to the collector from our hashmap
        SimpleImmutableEntry<Integer, String> pair = ordsToSuffixMap.get(nextOrd);
        remainingInstances = pair.getKey() - 1;
        BytesRef newBytesRef = new BytesRef(pair.getValue());
        currentValue = newBytesRef;
        return newBytesRef;
      }
    };
  }

  /**
   * Abstaction on top of building collectors to fetch values.
   *
   * @opensearch.internal
   */
  public interface CollectorSource extends Releasable {
    boolean needsScores();

    LeafBucketCollector getLeafCollector(
        IncludeExclude.StringFilter includeExclude,
        LeafReaderContext ctx,
        LeafBucketCollector sub,
        LongConsumer addRequestCircuitBreakerBytes,
        DuplicateTermsAggregator.CollectConsumer consumer
    ) throws IOException;
  }

  /**
   * Consumer for the collector
   *
   * @opensearch.internal
   */
  @FunctionalInterface
  public interface CollectConsumer {
    void accept(LeafBucketCollector sub, int doc, long owningBucketOrd, BytesRef bytes) throws IOException;
  }

  /**
   * Fetch values from a {@link ValuesSource}.
   *
   * @opensearch.internal
   */
  public static class ValuesSourceCollectorSource implements DuplicateTermsAggregator.CollectorSource {
    private final ValuesSource valuesSource;
    private final String separator;
    private final String aggField;

    public ValuesSourceCollectorSource(ValuesSource valuesSource, String separator, String aggField) {
      this.valuesSource = valuesSource;
      this.separator = separator;
      this.aggField = aggField;
    }

    @Override
    public boolean needsScores() {
      return valuesSource.needsScores();
    }

    @Override
    public LeafBucketCollector getLeafCollector(
        IncludeExclude.StringFilter includeExclude,
        LeafReaderContext ctx,
        LeafBucketCollector sub,
        LongConsumer addRequestCircuitBreakerBytes,
        DuplicateTermsAggregator.CollectConsumer consumer
    ) throws IOException {
      LeafReader reader = ctx.reader();
      SortedSetDocValues docValues = DocValues.getSortedSet(reader, aggField);
      SortedBinaryDocValues underscoreValues = DuplicateTermsAggregator.toString(docValues, separator);
      // This is where the conversion to our custom SortedBinaryDocValues occurs
      return new LeafBucketCollectorBase(sub, docValues) {

        /**
         * Collect the given doc in the given bucket.
         * Called once for every document matching a query, with the unbased document number.
         */
        @Override
        public void collect(int doc, long owningBucketOrd) throws IOException {
          // Useful log statement for viewing doc nums and bucket ord numbers, expect doc nums to go up and bucket ords to remain at 0
          // log.info("We are in the collect method, the doc num is " + doc + " the owningBucketOrd is " + owningBucketOrd);
          if (false == underscoreValues.advanceExact(doc)) {
            return;
          }
          int valuesCount = underscoreValues.docValueCount();
          // Useful log statement, expect this value to be the total number of values in a document
          // log.info("The valuesCount in getLeafCollector is " + valuesCount);
          for (int i = 0; i < valuesCount; ++i) {
            BytesRef bytes = underscoreValues.nextValue();
            if (includeExclude != null && false == includeExclude.accept(bytes)) {
              continue;
            }
            consumer.accept(sub, doc, owningBucketOrd, bytes); // this is adding values onto bucketOrds
          }
        }
      };
    }

    @Override
    public void close() {}
  }

  /**
   * Builds results for the standard {@code terms} aggregation.
   */
  class StandardTermsResults {
    private InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
      LocalBucketCountThresholds localBucketCountThresholds = context.asLocalBucketCountThresholds(duplicateBucketCountThresholds);
      InternalDuplicateTerms.InternalBucket[][] topBucketsPerOrd = buildTopBucketsPerOrd(owningBucketOrds.length);
      long[] otherDocCounts = new long[owningBucketOrds.length];
      for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
        collectZeroDocEntriesIfNeeded(owningBucketOrds[ordIdx]);
        int size = (int) Math.min(bucketOrds.size(), localBucketCountThresholds.getRequiredSize());

        PriorityQueue<InternalDuplicateTerms.InternalBucket> ordered = buildPriorityQueue(size);
        InternalDuplicateTerms.InternalBucket spare = null;
        // BytesKeyedBucketOrds.BucketOrdsEnum : An iterator for buckets inside a particular owningBucketOrd
        BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
        Supplier<InternalDuplicateTerms.InternalBucket> emptyBucketBuilder = emptyBucketBuilder(owningBucketOrds[ordIdx]); // build empty buckets
        // the primary logic occurs in this while loop IMPORTANT!
        while (ordsEnum.next()) { // iterate through ordinals
          // Here we get the bucketDocCount using utility method from BucketsAggregator.java, providing ord (Long) as key
          long docCount = bucketDocCount(ordsEnum.ord());
          otherDocCounts[ordIdx] += docCount;
          if (docCount < localBucketCountThresholds.getMinDocCount()) { // checks to see if doc count is below our requirements
            continue;
          }
          if (spare == null) {
            spare = emptyBucketBuilder.get();
          }
          updateBucket(spare, ordsEnum, docCount);
          spare = ordered.insertWithOverflow(spare);
        }

        topBucketsPerOrd[ordIdx] = buildBuckets(ordered.size());
        for (int i = ordered.size() - 1; i >= 0; --i) {
          topBucketsPerOrd[ordIdx][i] = ordered.pop();
          otherDocCounts[ordIdx] -= topBucketsPerOrd[ordIdx][i].getDocCount();
          finalizeBucket(topBucketsPerOrd[ordIdx][i]);
        }
      }

      buildSubAggs(topBucketsPerOrd);
      InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
      for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
        result[ordIdx] = buildResult(owningBucketOrds[ordIdx], otherDocCounts[ordIdx], topBucketsPerOrd[ordIdx]);
      }
      return result;
    }

    private final ValuesSource valuesSource;

    StandardTermsResults(ValuesSource valuesSource) {
      this.valuesSource = valuesSource;
    }

    String describe() {
      return "terms";
    }

    LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
      return primary;
    }

    void collectZeroDocEntriesIfNeeded(long owningBucketOrd) throws IOException {
      if (duplicateBucketCountThresholds.getMinDocCount() != 0) {
        return;
      }
      if (InternalOrder.isCountDesc(duplicateOrder) && bucketOrds.bucketsInOrd(owningBucketOrd) >= duplicateBucketCountThresholds.getRequiredSize()) {
        return;
      }
      // we need to fill-in the blanks
      for (LeafReaderContext ctx : context.searcher().getTopReaderContext().leaves()) {
        SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        // brute force
        for (int docId = 0; docId < ctx.reader().maxDoc(); ++docId) {
          if (values.advanceExact(docId)) {
            int valueCount = values.docValueCount();
            for (int i = 0; i < valueCount; ++i) {
              BytesRef term = values.nextValue();
              if (includeExclude == null || includeExclude.accept(term)) {
                bucketOrds.add(owningBucketOrd, term);
              }
            }
          }
        }
      }
    }

    Supplier<InternalDuplicateTerms.InternalBucket> emptyBucketBuilder(long owningBucketOrd) {
      return () -> new InternalDuplicateTerms.InternalBucket(new BytesRef(), 0, null, showTermDocCountError, 0, duplicateFormat);
    }

    PriorityQueue<InternalDuplicateTerms.InternalBucket> buildPriorityQueue(int size) {
      return new InternalBucketPriorityQueue<>(size, duplicatePartiallyBuiltBucketComparator);
    }

    void updateBucket(InternalDuplicateTerms.InternalBucket spare, BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum, long docCount) throws IOException {
      ordsEnum.readValue(spare.termBytes);
      spare.docCount = docCount;
      spare.bucketOrd = ordsEnum.ord();
    }

    InternalDuplicateTerms.InternalBucket[][] buildTopBucketsPerOrd(int size) {
      return new InternalDuplicateTerms.InternalBucket[size][];
    }

    InternalDuplicateTerms.InternalBucket[] buildBuckets(int size) {
      return new InternalDuplicateTerms.InternalBucket[size];
    }

    void finalizeBucket(InternalDuplicateTerms.InternalBucket bucket) {
      /*
       * termBytes contains a reference to the bytes held by the
       * bucketOrds which will be invalid once the aggregation is
       * closed so we have to copy it.
       */
      bucket.termBytes = BytesRef.deepCopyOf(bucket.termBytes);
    }

    void buildSubAggs(InternalDuplicateTerms.InternalBucket[][] topBucketsPerOrd) throws IOException {
      buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, a) -> b.aggregations = a);
    }

    InternalDuplicateTerms buildResult(long owningBucketOrd, long otherDocCount, InternalDuplicateTerms.InternalBucket[] topBuckets) {
      final BucketOrder reduceOrder;
      if (isKeyOrder(duplicateOrder) == false) {
        reduceOrder = InternalOrder.key(true);
        Arrays.sort(topBuckets, reduceOrder.comparator());
      } else {
        reduceOrder = duplicateOrder;
      }
      return new InternalDuplicateTerms(
          name,
          reduceOrder,
          duplicateOrder,
          metadata(),
          duplicateFormat,
          duplicateBucketCountThresholds.getShardSize(),
          showTermDocCountError,
          otherDocCount,
          Arrays.asList(topBuckets),
          0,
          duplicateBucketCountThresholds
      );
    }

    InternalDuplicateTerms buildEmptyResult() {
      return buildEmptyTermsAggregation();
    }
  }
}

