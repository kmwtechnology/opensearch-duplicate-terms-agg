Opensearch Duplicate Terms Aggregation Plugin
=========================================

This plugin adds the ability to perform aggregations on values that possess counts. This plug-in aims to perform this task
in the most performant way possible with regards to index memory size and query time. Each value provided is expected to contain
a separator (defaulted to "_") that will delimit the valueName and valueCount i.e. foo_3 --> [foo, foo, foo].

This is a Terms Aggregation

Installation
------------

1. Clone repository `master` branch

2. Install `gradle` and run `gradle wrapper` at the top-level

3. Perform `./gradlew build` or `./gradlew assemble` (no tests) at top-level

4. Perform `./gradlew check` at top-level to run tests and package the plug-in

5. Navigate to `build/distributions` directory within project and confirm that `duplicate-terms-plugin.jar` and `duplicate-terms-plugin.zip` both exist

6. Copy absolute path of aforementioned `duplicate-terms-plugin.zip`

7. Clone version 2.11.1 of OS from OS repo or use your own 

8. Perform `./gradlew localDistro` at top-level

9. `cd` into `build/distribution/local/opensearch-2.11.1-SNAPSHOT` 

10. Perform `bin/opensearch-plugin install file://<absolute-path-from-step-5>` e.g. `file:///Users/abijitrangesh/Documents/GitHub/underscore-duplicate-term-aggregation/build/distributions/duplicate-terms-plugin.zip`
Notice the number of slashes after `file:`

11. Your plug-in should be working, if we want to re-install the plug-in, first remove the plug-in via `bin/OpenSearch-plugin remove duplicate-terms-plugin` and then run the install command from Step 9

12. To run your OS instance, perform `bin/opensearch` within 2.11.1 distribution of OS (from Step 8)


Development Environment Setup
------------

Follow steps 1 & 2 of installation, 

Duplicate Terms Aggregation
--------------------------

### Parameters

- `field` : field to aggregate on
- `separator` : separator for path hierarchy (default to "_")
- `order` : order parameter to define how to sort result. Allowed parameters are `_key`, `_count` or sub aggregation name. Default to {"_count": "desc}.
- `size`: size parameter to define how many buckets should be returned. Default to 10.
- `shard_size`: how many buckets returned by each shards. Set to size if smaller, default to size if the search request needs to go to a single shard, and (size * 1.5 + 10) otherwise (more information here: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html#_shard_size_3).
- `agg_field`: field to aggregate on
- `min_doc_count`: Return buckets containing at least `min_doc_count` document. Default to 0


Examples
-------

#### String field

```
# Add data:
curl -XPUT "http://localhost:9200/favorite-foods-underscore2" -H 'Content-Type: application/json' -d'  	 
{
  "mappings": {
	"properties": {
  		"favorite_foods": {
    			"type": "keyword"
  		}
	}
  }
}'
curl -XPUT "http://localhost:9200/favorite-foods-underscore2/_doc/1" -H 'Content-Type: application/json' -d'
{
  "favorite_foods": ["foo_3", "bar_5", "baz_2"]
}'


curl -XPUT "http://localhost:9200/favorite-foods-underscore2/_doc/2" -H 'Content-Type: application/json' -d'
{
  "favorite_foods": ["foo_1"]
}'

curl -XPUT "http://localhost:9200/favorite-foods-underscore2/_doc/3" -H 'Content-Type: application/json' -d'
{
  "favorite_foods": ["bar_1"]
}'


# Duplicate terms aggregation plug-in call :
curl -XGET "http://localhost:9200/favorite-foods-underscore2/_search" -H 'Content-Type: application/json' -d'
{
  "size": 0,
  "aggregations": {
	"desc_frequency": {
  	    "duplicate_terms": {
            "field": "favorite_foods",
            "size": 250,
            "separator": "_",
            "agg_field": "favorite_foods"
        }
	}
  }
}'



Result :
{
    "took":153,
    "timed_out":false,
    "_shards":{
        "total":1,
        "successful":1,
        "skipped":0,
        "failed":0
    },
    "hits":{
        "total":{
            "value":3,"relation":"eq"
        },
        "max_score":null,
        "hits":[]
    },
    "aggregations":{
        "desc_frequency":{
            "doc_count_error_upper_bound":0,
            "sum_other_doc_count":0,
            "buckets":[
                {
                    "key":"bar",
                    "doc_count":6
                },
                {
                    "key":"foo",
                    "doc_count":4
                },{
                    "key":"baz",
                    "doc_count":2
                }
            ]
        }
    }
}
```

License
-------

This software is under The MIT License (MIT).


Project Structure
-------

```
`-- src
    |-- main
    |   `-- java
    |       `-- org
    |           `-- opensearch
    |               `-- search
    |                   `-- aggregations
    |                       `-- bucket
    |                           `-- terms
    |                               `-- DuplicateTermsAggregationPlugin.java
    |                               `-- DuplicateAbstractStringTermsAggregator.java
    |                               `-- DuplicateTermsAggregationBuilder.java
    |                               `-- DuplicateTermsAggregator.java
    |                               `-- DuplicateTermsAggregatorFactory.java
    |                               `-- DuplicateTermsAggregatorSupplier.java
    |                               `-- InternalBucketPriorityQueue.java
    |                               `-- InternalDuplicateTerms.java
    |-- test
    |   `-- java
    |       `-- org
    |           `-- opensearch
    |               `-- search
    |                   `-- aggregations
    |                       `-- bucket
    |                            `-- terms
    |                                |-- DuplicateTermsAggregationPluginIT.java
    |                                `-- DuplicateTermsAggregationTests.java
    `-- yamlRestTest
    |   `-- java
    |       `-- org
    |           `-- opensearch
    |               `-- search
    |                   `-- aggregations
    |                       `-- bucket
    |                            `-- terms
    |                                `-- DuplicateTermsAggregationClientYamlTestSuiteIT.java
    |   `-- resources
    |       `-- rest-api-spec
    |           `-- test
    |               `-- 10_basic.yml
`-- build.gradle
`-- settings.gradle
`-- gradlew
`-- NOTICE.txt
`-- README.md
`-- LICENSE.txt
`-- gradlew.bat
```

