package org.opensearch.search.aggregations.bucket;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BytesRefHash;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.InternalDuplicateTermAggregation.InternalBucket;
import org.opensearch.search.aggregations.metrics.MetricsAggregator;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

public class DuplicateTermAggregator extends BucketsAggregator {

  public DuplicateTermAggregator(String name,
      AggregatorFactories factories,
      SearchContext context,
      ValuesSource.Numeric valuesSource,
      BucketOrder order,
      long minDocCount,
      BucketCountThresholds bucketCountThresholds,
      Aggregator parent,
      CardinalityUpperBound cardinalityUpperBound,
      Map<String,  Object> metadata
  ) throws IOException {
    super(name, factories, context, parent, cardinalityUpperBound, metadata);
    this.valuesSource = valuesSource;
    this.minDocCount = minDocCount;
    bucketOrds = new BytesRefHash(1, context.bigArrays());
    this.bucketCountThresholds = bucketCountThresholds;
    order.validate(this);
    this.order = order;
    this.partiallyBuiltBucketComparator = order == null ? null : order.partiallyBuiltBucketComparator(b -> b.bucketOrd, this);
  }

  public static class BucketCountThresholds implements Writeable, ToXContentFragment {
    private int requiredSize;
    private int shardSize;

    public BucketCountThresholds(int requiredSize, int shardSize) {
      this.requiredSize = requiredSize;
      this.shardSize = shardSize;
    }

    /**
     * Read from a stream.
     */
    public BucketCountThresholds(StreamInput in) throws IOException {
      requiredSize = in.readInt();
      shardSize = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
      out.writeInt(requiredSize);
      out.writeInt(shardSize);
    }

    public BucketCountThresholds(DuplicateTermAggregator.BucketCountThresholds bucketCountThresholds) {
      this(bucketCountThresholds.requiredSize, bucketCountThresholds.shardSize);
    }

    public void ensureValidity() {
      // shard_size cannot be smaller than size as we need to at least fetch size entries from every shards in order to return size
      if (shardSize < requiredSize) {
        setShardSize(requiredSize);
      }

      if (requiredSize <= 0 || shardSize <= 0) {
        throw new OpenSearchException("parameters [required_size] and [shard_size] must be >0 in path-hierarchy aggregation.");
      }
    }

    public int getRequiredSize() {
      return requiredSize;
    }

    public void setRequiredSize(int requiredSize) {
      this.requiredSize = requiredSize;
    }

    public int getShardSize() {
      return shardSize;
    }

    public void setShardSize(int shardSize) {
      this.shardSize = shardSize;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
      builder.field(DuplicateTermAggregationBuilder.SIZE_FIELD.getPreferredName(), requiredSize);
      if (shardSize != -1) {
        builder.field(DuplicateTermAggregationBuilder.SHARD_SIZE_FIELD.getPreferredName(), shardSize);
      }
      return builder;
    }

    @Override
    public int hashCode() {
      return Objects.hash(requiredSize, shardSize);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      DuplicateTermAggregator.BucketCountThresholds other = (DuplicateTermAggregator.BucketCountThresholds) obj;
      return Objects.equals(requiredSize, other.requiredSize)
          && Objects.equals(shardSize, other.shardSize);
    }
  }

  private final ValuesSource.Numeric valuesSource;
  private final BytesRefHash bucketOrds;
  private final BucketOrder order;
  private final long minDocCount;
  private final BucketCountThresholds bucketCountThresholds;
  protected final Comparator<InternalBucket> partiallyBuiltBucketComparator;

  /**
   * The collector collects the docs, including or not some score (depending of the including of a Scorer) in the
   * collect() process.
   *
   * The LeafBucketCollector is a "Per-leaf bucket collector". It collects docs for the account of buckets.
   */
  @Override
  public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
    if (valuesSource == null) {
      return LeafBucketCollector.NO_OP_COLLECTOR;
    }
    final SortedNumericDocValues values = valuesSource.longValues(ctx);
    return new LeafBucketCollectorBase(sub, values) {

      @Override
      public void collect(int doc, long bucket) throws IOException {
        assert bucket == 0;
        if (values.advanceExact(doc)) {
          final int valuesCount = values.docValueCount();

          for (int i = 0; i < valuesCount; ++i) {
            long value = values.nextValue();
            String path = "";
            /**
             * this is the brunt of the logic
             */
//            for (DuplicateTermAggregationBuilder.RoundingInfo roundingInfo: roundingsInfo) {
//              // A little hacky: Add a microsecond to avoid collision between min dates interval
//              // Since interval cannot be set to microsecond, it is not a problem
//              long roundedValue = roundingInfo.rounding.round(value);
//              path += roundingInfo.format.format(roundedValue).toString();
//              long bucketOrd = bucketOrds.add(new BytesRef(path));
//              if (bucketOrd < 0) { // already seen
//                bucketOrd = -1 - bucketOrd;
//                collectExistingBucket(sub, doc, bucketOrd);
//              } else {
//                collectBucket(sub, doc, bucketOrd);
//              }
//              path += "/";
//            }
          }
        }
      }
    };
  }

  @Override
  public InternalAggregation[] buildAggregations(long[] owningBucketOrdinals) throws IOException {

    InternalDuplicateTermAggregation.InternalBucket[][] topBucketsPerOrd = new InternalDuplicateTermAggregation.InternalBucket[owningBucketOrdinals.length][];
    InternalDuplicateTermAggregation[] results = new InternalDuplicateTermAggregation[owningBucketOrdinals.length];

    for (int ordIdx = 0; ordIdx < owningBucketOrdinals.length; ordIdx++) {
      assert owningBucketOrdinals[ordIdx] == 0;

      // build buckets and store them sorted
      final int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());

      PathSortedTree<String, InternalDuplicateTermAggregation.InternalBucket> pathSortedTree = new PathSortedTree<>(order.comparator(), size);

      InternalDuplicateTermAggregation.InternalBucket spare;
      for (int i = 0; i < bucketOrds.size(); i++) {
        spare = new InternalDuplicateTermAggregation(0, null, null, null, 0, null);

        BytesRef term = new BytesRef();
        bucketOrds.get(i, term);
        String[] paths = term.utf8ToString().split("/", -1);

        spare.paths = paths;
        spare.key = term;
        spare.level = paths.length - 1;
        spare.name = paths[spare.level];
        spare.docCount = bucketDocCount(i);
        spare.bucketOrd = i;

        pathSortedTree.add(spare.paths, spare);
      }

      // Get the top buckets
      topBucketsPerOrd[ordIdx] = new InternalDuplicateTermAggregation.InternalBucket[size];
      long otherHierarchyNodes = pathSortedTree.getFullSize();
      Iterator<InternalBucket> iterator = pathSortedTree.consumer();
      for (int i = 0; i < size; i++) {
        final InternalDuplicateTermAggregation.InternalBucket bucket = iterator.next();
        topBucketsPerOrd[ordIdx][i] = bucket;
        otherHierarchyNodes -= 1;
      }

      results[ordIdx] = new InternalDuplicateTermAggregation(name, Arrays.asList(topBucketsPerOrd[ordIdx]), order,
          minDocCount, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getShardSize(),
          otherHierarchyNodes, metadata());
    }

    // Build sub-aggregations for pruned buckets
    buildSubAggsForAllBuckets(
        topBucketsPerOrd,
        b -> b.bucketOrd,
        (b, aggregations) -> b.aggregations = aggregations
    );

    return results;
  }

  @Override
  public InternalAggregation buildEmptyAggregation() {
    return new InternalDuplicateTermAggregation(name, null, order, minDocCount, bucketCountThresholds.getRequiredSize(),
        bucketCountThresholds.getShardSize(), 0, metadata());
  }

  @Override
  protected void doClose() {
    Releasables.close(bucketOrds);
  }
}
