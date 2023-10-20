package org.opensearch.search.aggregations.bucket;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.opensearch.search.aggregations.KeyComparable;
import org.opensearch.search.aggregations.bucket.InternalDuplicateTermAggregation.InternalBucket;

/*
 Returns builder and reduces the buckets
 */
public class InternalDuplicateTermAggregation extends InternalMultiBucketAggregation<InternalDuplicateTermAggregation,
    InternalBucket> {

  protected static final ParseField SUM_OF_OTHER_HIERARCHY_NODES = new ParseField("sum_other_hierarchy_nodes");

  public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucket implements
      KeyComparable<InternalBucket> {

    BytesRef termBytes;
    long bucketOrd;
    protected String[] paths;
    protected long docCount;
    protected InternalAggregations aggregations;
    protected int level;
    protected int minDepth;
    protected String basename;

    public InternalBucket(long docCount, InternalAggregations aggregations, String basename,
        BytesRef term, int level, int minDepth, String[] paths) {
      termBytes = term;
      this.docCount = docCount;
      this.aggregations = aggregations;
      this.level = level;
      this.minDepth = minDepth;
      this.basename = basename;
      this.paths = paths;
    }

    /**
     * Read from a stream.
     */
    public InternalBucket(StreamInput in) throws IOException {
      termBytes = in.readBytesRef();
      docCount = in.readLong();
      aggregations = InternalAggregations.readFrom(in);
      level = in.readInt();
      minDepth = in.readInt();
      basename = in.readString();
      int pathsSize = in.readInt();
      paths = new String[pathsSize];
      for (int i=0; i < pathsSize; i++) {
        paths[i] = in.readString();
      }
    }

    /**
     * Write to a stream.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
      out.writeBytesRef(termBytes);
      out.writeLong(docCount);
      aggregations.writeTo(out);
      out.writeInt(level);
      out.writeInt(minDepth);
      out.writeString(basename);
      out.writeInt(paths.length);
      for (String path: paths) {
        out.writeString(path);
      }
    }

    @Override
    public String getKey() {
      return termBytes.utf8ToString();
    }

    @Override
    public String getKeyAsString() {
      return termBytes.utf8ToString();
    }

    @Override
    public int compareKey(InternalBucket other) {
      return termBytes.compareTo(other.termBytes);
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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
      builder.startObject();
      builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
      aggregations.toXContentInternal(builder, params);
      builder.endObject();
      return builder;
    }
  }

  private List<InternalDuplicateTermAggregation.InternalBucket> buckets;


}
