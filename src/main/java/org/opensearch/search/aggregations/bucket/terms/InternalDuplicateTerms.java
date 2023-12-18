package org.opensearch.search.aggregations.bucket.terms;

import static org.opensearch.search.aggregations.InternalOrder.isKeyAsc;
import static org.opensearch.search.aggregations.InternalOrder.isKeyOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationExecutionException;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.KeyComparable;
import org.opensearch.search.aggregations.bucket.IteratorAndCurrent;
import org.opensearch.search.aggregations.bucket.LocalBucketCountThresholds;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;

public class InternalDuplicateTerms extends InternalMultiBucketAggregation<InternalDuplicateTerms, InternalDuplicateTerms.InternalBucket> {

  // Uncomment for logging
  // private static final Logger log = LogManager.getLogger(InternalDuplicateTerms.class);
  public static final String NAME = "duplicateterms";

  /**
   * Bucket for internal duplicate terms
   *
   * @opensearch.internal
   */
  public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucket implements
      KeyComparable<InternalBucket> {
    BytesRef termBytes;
    long bucketOrd;
    long docCount;
    InternalAggregations aggregations;
    boolean showDocCountError;
    long docCountError;
    DocValueFormat format;
    public InternalBucket(
        BytesRef term,
        long docCount,
        InternalAggregations aggregations,
        boolean showDocCountError,
        long docCountError,
        DocValueFormat format
    ) {
      super();
      this.termBytes = term;
      this.docCount = docCount;
      this.aggregations = aggregations;
      this.showDocCountError = showDocCountError;
      this.docCountError = docCountError;
      this.format = format;
    }

    /**
     * Read from a stream.
     */
    public InternalBucket(StreamInput in, DocValueFormat format, boolean showDocCountError) throws IOException {
      super();
      this.termBytes = in.readBytesRef();
      this.docCount = in.readLong();
      this.aggregations = InternalAggregations.readFrom(in);
      this.showDocCountError = showDocCountError;
      this.docCountError = in.readLong();
      this.format = format;
    }

    @Override
    public Object getKey() {
      return getKeyAsString();
    }

    @Override
    public String getKeyAsString() {
      return format.format(termBytes).toString();
    }

    @Override
    public long getDocCount() {
      return docCount;
    }

    @Override
    public Aggregations getAggregations() {
      return aggregations;
    }

    @Override
    public boolean equals(Object obj) {
      return super.equals(obj) && Objects.equals(termBytes, ((StringTerms.Bucket) obj).termBytes);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), termBytes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
      out.writeBytesRef(termBytes);
      out.writeLong(docCount);
      aggregations.writeTo(out);
      out.writeBoolean(showDocCountError);
      out.writeLong(docCount);
      format.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
      builder.startObject();
      builder.field(CommonFields.KEY.getPreferredName(), getKeyAsString());
      builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
      if (showDocCountError) {
        builder.field(InternalTerms.DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName(), docCountError);
      }
      aggregations.toXContentInternal(builder, params);
      builder.endObject();
      return builder;
    }

    @Override
    public int compareKey(InternalBucket other) {
      return termBytes.compareTo(other.termBytes);
    }
  }

  private BucketOrder reduceOrder;
  private BucketOrder order;
  private DocValueFormat format;
  private int shardSize;
  private boolean showTermDocCountError;
  private long otherDocCount;
  private List<InternalDuplicateTerms.InternalBucket> buckets;

  private long docCountError;
  private TermsAggregator.BucketCountThresholds bucketCountThresholds;

  public InternalDuplicateTerms(
      String name,
      BucketOrder reduceOrder,
      BucketOrder order,
      Map<String, Object> metadata,
      DocValueFormat format,
      int shardSize,
      boolean showTermDocCountError,
      long otherDocCount,
      List<InternalDuplicateTerms.InternalBucket> buckets,
      long docCountError,
      TermsAggregator.BucketCountThresholds bucketCountThresholds
  ) {
    super(name, metadata);
    this.reduceOrder = reduceOrder;
    this.order = order;
    this.format = format;
    this.shardSize = shardSize;
    this.showTermDocCountError = showTermDocCountError;
    this.otherDocCount = otherDocCount;
    this.buckets = buckets;
    this.docCountError = docCountError;
    this.bucketCountThresholds = bucketCountThresholds;
  }

  /**
   * Read from a stream.
   */
  public InternalDuplicateTerms(StreamInput in) throws IOException {
    super(in);
    reduceOrder = InternalOrder.Streams.readOrder(in);
    order = InternalOrder.Streams.readOrder(in);
    format = DocValueFormat.BINARY;
    shardSize = readSize(in);
    showTermDocCountError = in.readBoolean();
    otherDocCount = in.readLong();
    docCountError = in.readLong();
    int bucketsSize = in.readInt();
    this.buckets = new ArrayList<>(bucketsSize);
    for (int i=0; i<bucketsSize; i++) {
      this.buckets.add(new InternalBucket(in, format, showTermDocCountError));
    }
  }

  @Override
  protected void doWriteTo(StreamOutput out) throws IOException {

  }

  // Sums any doc counts that weren't collected
  public long getSumOfOtherDocCounts() {
    return otherDocCount;
  }

  private long getDocCountError(InternalDuplicateTerms terms, ReduceContext reduceContext) {
    int size = terms.getBuckets().size();
    // doc_count_error is always computed at the coordinator based on the buckets returned by the shards. This should be 0 during the
    // shard level reduce as no buckets are being pruned at this stage.
    if (reduceContext.isSliceLevel() || size == 0 || size < terms.shardSize || isKeyOrder(terms.order)) {
      return 0;
    } else if (InternalOrder.isCountDesc(terms.order)) {
      if (terms.docCountError > 0) {
        // If there is an existing docCountError for this agg then
        // use this as the error for this aggregation
        return terms.docCountError;
      } else {
        // otherwise use the doc count of the last term in the
        // aggregation
        return terms.getBuckets().stream().mapToLong(MultiBucketsAggregation.Bucket::getDocCount).min().getAsLong();
      }
    } else {
      return -1;
    }
  }

  private BucketOrder getReduceOrder(List<InternalAggregation> aggregations) {
    BucketOrder thisReduceOrder = null;
    for (InternalAggregation aggregation : aggregations) {
      @SuppressWarnings("unchecked")
      InternalDuplicateTerms terms = (InternalDuplicateTerms) aggregation;
      if (terms.getBuckets().size() == 0) {
        continue;
      }
      if (thisReduceOrder == null) {
        thisReduceOrder = terms.reduceOrder;
      } else if (thisReduceOrder.equals(terms.reduceOrder) == false) {
        return order;
      }
    }
    return thisReduceOrder != null ? thisReduceOrder : order;
  }

  // This is the default and what the aggregation will likely be doing, uses merge sort to reduce the buckets
  private List<InternalBucket> reduceMergeSort(List<InternalAggregation> aggregations, BucketOrder thisReduceOrder, ReduceContext reduceContext) {
    assert isKeyOrder(thisReduceOrder);
    final Comparator<Bucket> cmp = thisReduceOrder.comparator();
    final PriorityQueue<IteratorAndCurrent<InternalBucket>> pq = new PriorityQueue<IteratorAndCurrent<InternalBucket>>(aggregations.size()) {
      @Override
      protected boolean lessThan(IteratorAndCurrent<InternalBucket> a, IteratorAndCurrent<InternalBucket> b) {
        return cmp.compare(a.current(), b.current()) < 0;
      }
    };
    for (InternalAggregation aggregation : aggregations) {
      @SuppressWarnings("unchecked")
      InternalDuplicateTerms terms = (InternalDuplicateTerms) aggregation;
      if (terms.getBuckets().isEmpty() == false) {
        assert reduceOrder.equals(reduceOrder);
        pq.add(new IteratorAndCurrent(terms.getBuckets().iterator()));
      }
    }
    List<InternalBucket> reducedBuckets = new ArrayList<>();
    // list of buckets coming from different shards that have the same key
    List<InternalBucket> currentBuckets = new ArrayList<>();
    InternalBucket lastBucket = null;
    while (pq.size() > 0) {
      final IteratorAndCurrent<InternalBucket> top = pq.top();
      assert lastBucket == null || cmp.compare(top.current(), lastBucket) >= 0;
      if (lastBucket != null && cmp.compare(top.current(), lastBucket) != 0) {
        // the key changes, reduce what we already buffered and reset the buffer for current buckets
        final InternalBucket reduced = reduceBucket(currentBuckets, reduceContext);
        reducedBuckets.add(reduced);
        currentBuckets.clear();
      }
      lastBucket = top.current();
      currentBuckets.add(top.current());
      if (top.hasNext()) {
        top.next();
        assert cmp.compare(top.current(), lastBucket) > 0 : "shards must return data sorted by key";
        pq.updateTop();
      } else {
        pq.pop();
      }
    }

    if (currentBuckets.isEmpty() == false) {
      final InternalBucket reduced = reduceBucket(currentBuckets, reduceContext);
      reducedBuckets.add(reduced);
    }
    return reducedBuckets;
  }

  // In case the order is not defined per keys, then we use reduce Legacy, which is less performant
  private List<InternalBucket> reduceLegacy(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
    Map<Object, List<InternalBucket>> bucketMap = new HashMap<>();
    for (InternalAggregation aggregation : aggregations) {
      @SuppressWarnings("unchecked")
      InternalDuplicateTerms terms = (InternalDuplicateTerms) aggregation;
      if (terms.getBuckets().isEmpty() == false) {
        for (InternalBucket bucket : terms.getBuckets()) {
          List<InternalBucket> bucketList = bucketMap.get(bucket.getKey());
          if (bucketList == null) {
            bucketList = new ArrayList<>();
            bucketMap.put(bucket.getKey(), bucketList);
          }
          bucketList.add(bucket);
        }
      }
    }
    List<InternalBucket> reducedBuckets = new ArrayList<>();
    for (List<InternalBucket> sameTermBuckets : bucketMap.values()) {
      final InternalBucket b = reduceBucket(sameTermBuckets, reduceContext);
      reducedBuckets.add(b);
    }
    return reducedBuckets;
  }

  private InternalDuplicateTerms.InternalBucket[] createBucketsArray(int size) {
    return new InternalDuplicateTerms.InternalBucket[size];
  }

  @Override
  public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
    LocalBucketCountThresholds localBucketCountThresholds = reduceContext.asLocalBucketCountThresholds(bucketCountThresholds);
    long sumDocCountError = 0;
    long otherDocCount = 0;
    InternalDuplicateTerms referenceTerms = null;
    for (InternalAggregation aggregation : aggregations) {
      @SuppressWarnings("unchecked")
      InternalDuplicateTerms terms = (InternalDuplicateTerms) aggregation;
      if (referenceTerms == null && aggregation.getClass().equals(UnmappedTerms.class) == false) {
        referenceTerms = terms;
      }
      if (referenceTerms != null
          && referenceTerms.getClass().equals(terms.getClass()) == false
          && terms.getClass().equals(UnmappedTerms.class) == false) {
        // control gets into this loop when the same field name against which the query is executed
        // is of different types in different indices.
        throw new AggregationExecutionException(
            "Merging/Reducing the aggregations failed when computing the aggregation ["
                + referenceTerms.getName()
                + "] because the field you gave in the aggregation query existed as two different "
                + "types in two different indices"
        );
      }
      otherDocCount += terms.getSumOfOtherDocCounts();
      final long thisAggDocCountError = getDocCountError(terms, reduceContext);
      if (sumDocCountError != -1) {
        if (thisAggDocCountError == -1) {
          sumDocCountError = -1;
        } else {
          sumDocCountError += thisAggDocCountError;
        }
      }
      docCountError = thisAggDocCountError;
      for (InternalBucket bucket : terms.getBuckets()) {
        // If there is already a doc count error for this bucket
        // subtract this aggs doc count error from it to make the
        // new value for the bucket. This then means that when the
        // final error for the bucket is calculated below we account
        // for the existing error calculated in a previous reduce.
        // Note that if the error is unbounded (-1) this will be fixed
        // later in this method.
        bucket.docCountError = docCountError - thisAggDocCountError;
      }
    }


    final List<InternalDuplicateTerms.InternalBucket> reducedBuckets;
        /*
          Buckets returned by a partial reduce or a shard response are sorted by key.
          That allows to perform a merge sort when reducing multiple aggregations together.
          For backward compatibility, we disable the merge sort and use ({@link InternalTerms#reduceLegacy} if any of
          the provided aggregations use a different {@link InternalTerms#reduceOrder}.
         */
    BucketOrder thisReduceOrder = getReduceOrder(aggregations);
    if (isKeyOrder(thisReduceOrder)) {
      // extract the primary sort in case this is a compound order.
      thisReduceOrder = InternalOrder.key(isKeyAsc(thisReduceOrder) ? true : false);
      reducedBuckets = reduceMergeSort(aggregations, thisReduceOrder, reduceContext);
    } else {
      reducedBuckets = reduceLegacy(aggregations, reduceContext);
    }
    final InternalBucket[] list;
    if (reduceContext.isFinalReduce() || reduceContext.isSliceLevel()) {
      final int size = Math.min(localBucketCountThresholds.getRequiredSize(), reducedBuckets.size());
      // final comparator
      final InternalBucketPriorityQueue<InternalBucket> ordered = new InternalBucketPriorityQueue<InternalBucket>(size, order.comparator());
      for (InternalBucket bucket : reducedBuckets) {
        if (sumDocCountError == -1) {
          bucket.docCountError = -1;
        } else {
          final long finalSumDocCountError = sumDocCountError;
          bucket.docCountError = docCountError + finalSumDocCountError;
        }
        if (bucket.getDocCount() >= localBucketCountThresholds.getMinDocCount()) {
          InternalBucket removed = ordered.insertWithOverflow(bucket);
          if (removed != null) {
            otherDocCount += removed.getDocCount();
            reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(removed));
          } else {
            reduceContext.consumeBucketsAndMaybeBreak(1);
          }
        } else {
          reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(bucket));
        }
      }
      list = createBucketsArray(ordered.size());
      for (int i = ordered.size() - 1; i >= 0; i--) {
        list[i] = ordered.pop();
      }
    } else {
      // we can prune the list on partial reduce if the aggregation is ordered by key
      // and not filtered (minDocCount == 0)
      int size = isKeyOrder(order) && localBucketCountThresholds.getMinDocCount() == 0
          ? Math.min(localBucketCountThresholds.getRequiredSize(), reducedBuckets.size())
          : reducedBuckets.size();
      list = createBucketsArray(size);
      for (int i = 0; i < size; i++) {
        reduceContext.consumeBucketsAndMaybeBreak(1);
        list[i] = reducedBuckets.get(i);
        if (sumDocCountError == -1) {
          list[i].docCountError = -1;
        } else {
          final long fSumDocCountError = sumDocCountError;
          list[i].docCountError = docCountError + fSumDocCountError;
        }
      }
    }
    long docCountError;
    if (sumDocCountError == -1) {
      docCountError = -1;
    } else {
      docCountError = aggregations.size() == 1 ? 0 : sumDocCountError;
    }

    // Shards must return buckets sorted by key, so we apply the sort here in shard level reduce
    if (reduceContext.isSliceLevel()) {
      Arrays.sort(list, thisReduceOrder.comparator());
    }
    return create(Arrays.asList(list));
  }

  // Printing out the buckets and info related to buckets
  @Override
  public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
    builder.field("doc_count_error_upper_bound", docCountError);
    builder.field("sum_other_doc_count", otherDocCount);
    builder.startArray("buckets");
    for (InternalBucket bucket : buckets) {
//      log.info("this is the bucket we are printing ", bucket.toXContent(builder, params));
      bucket.toXContent(builder, params);
    }
    builder.endArray();
    return builder;
  }

  @Override
  public InternalDuplicateTerms.InternalBucket createBucket(InternalAggregations aggregations,
      InternalDuplicateTerms.InternalBucket prototype) {
    return new InternalBucket(prototype.termBytes, prototype.docCount, prototype.aggregations, prototype.showDocCountError,
        docCountError, prototype.format);
  }

  @Override
  protected InternalBucket reduceBucket(List<InternalBucket> buckets, ReduceContext context) {
    assert buckets.size() > 0;
    long docCount = 0;
    // For the per term doc count error we add up the errors from the
    // shards that did not respond with the term. To do this we add up
    // the errors from the shards that did respond with the terms and
    // subtract that from the sum of the error from all shards
    long docCountError = 0;
    List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
    for (InternalDuplicateTerms.InternalBucket bucket : buckets) {
      docCount += bucket.getDocCount();
      if (docCountError != -1) {
        if (bucket.showDocCountError == false || bucket.docCountError == -1) {
          docCountError = -1;
        } else {
          docCountError += bucket.docCountError;
        }
      }
      aggregationsList.add((InternalAggregations) bucket.getAggregations());
    }
    InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
    return createBucket(aggs, buckets.get(0));
  }

  @Override
  public List<InternalDuplicateTerms.InternalBucket> getBuckets() {
    return buckets;
  }

  @Override
  public String getWriteableName() {
    return NAME;
  }

  @Override
  public InternalDuplicateTerms create(List<InternalDuplicateTerms.InternalBucket> buckets) {
    return new InternalDuplicateTerms(
        name,
        reduceOrder,
        order,
        getMetadata(),
        format,
        shardSize,
        showTermDocCountError,
        otherDocCount,
        buckets,
        docCountError,
        bucketCountThresholds
    );
  }
}
