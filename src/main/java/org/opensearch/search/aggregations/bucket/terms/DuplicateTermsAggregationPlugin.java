/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.aggregations.bucket.terms;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;


public class DuplicateTermsAggregationPlugin extends Plugin implements SearchPlugin {
  @Override
  public List<AggregationSpec> getAggregations() {
    ArrayList<SearchPlugin.AggregationSpec> aggregations = new ArrayList<>();
    aggregations.add(
        new AggregationSpec(
            DuplicateTermsAggregationBuilder.NAME, // name of aggregation and how to call it
            DuplicateTermsAggregationBuilder::new, // entry point of aggregator
            DuplicateTermsAggregationBuilder.PARSER) // instructions for how to parse the aggregation
            .setAggregatorRegistrar(DuplicateTermsAggregationBuilder::registerAggregators) // registers aggregator with OS
            .addResultReader(InternalDuplicateTerms::new) // instructions on how to read the results of the buckets
    );
    return aggregations;
    }
}
