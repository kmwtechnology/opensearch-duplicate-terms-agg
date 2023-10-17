/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin;

import java.util.Arrays;
import java.util.List;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;


public class UniqueCountAggregationPlugin extends Plugin implements SearchPlugin {
//  @Override
//  public List<AggregationSpec> getAggregations() {
//    return Arrays.asList(
//        new AggregationSpec(DigestAggregationBuilder.NAME,
//            DigestAggregationBuilder::new,
//            DigestAggregationBuilder.PARSER)
//            .addResultReader(InternalDigest::new)
//            .setAggregatorRegistrar(DigestAggregationBuilder::registerAggregators),
//        new AggregationSpec(RawDigestAggregationBuilder.NAME,
//            RawDigestAggregationBuilder::new,
//            RawDigestAggregationBuilder.PARSER)
//            .addResultReader(InternalDigest::new)
//            .setAggregatorRegistrar(RawDigestAggregationBuilder::registerAggregators)
//    );
//  }
}
