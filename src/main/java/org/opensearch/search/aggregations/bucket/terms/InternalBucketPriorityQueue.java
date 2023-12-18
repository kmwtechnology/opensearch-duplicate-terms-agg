package org.opensearch.search.aggregations.bucket.terms;

import java.util.Comparator;
import org.apache.lucene.util.PriorityQueue;

// Custom version of BucketPriorityQueue that works with InternalDuplicateTerms.InternalBucket
public class InternalBucketPriorityQueue<B extends InternalDuplicateTerms.InternalBucket> extends PriorityQueue<B> {

  private final Comparator<? super B> comparator;

  public InternalBucketPriorityQueue(int size, Comparator<? super B> comparator) {
    super(size);
    this.comparator = comparator;
  }

  @Override
  protected boolean lessThan(B a, B b) {
    return comparator.compare(a, b) > 0; // reverse, since we reverse again when adding to a list
  }
}
