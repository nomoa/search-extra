Extra Queries and Filters [![Build Status](https://integration.wikimedia.org/ci/buildStatus/icon?job=search-extra)](https://integration.wikimedia.org/ci/job/search-extra)
=========================

The plan is for this to include any extra queries, filters, native scripts,
score functions, and anything else we think we end up creating to make search
nice for Wikimedia.  At this point it only contains:

Filters:
* [source_regex](docs/source_regex.md) - An nGram accelerated regular
expression filter that is generally much much faster than sequentially checking
all documents.
* [id_hash_mod](docs/id_hash_mod.md) - Filter used to select all documents
independantly. For example, it can be used by multiple processes to reindex
all documents without any interprocess communication. Added in 1.5.0, 1.4.1,
and 1.3.0.

Queries:
* [safer](docs/safer.md) - Wraps other queries and analyzes them for
potentially expensive constructs.  Expensive constructs either cause errors to
be sent back to the user or are degraded into cheaper, less precise constructs.

Native Scripts:
* [super_detect_noop](docs/super_detect_noop.md) - Like ```detect_noop``` but
supports configurable sloppiness. New in 1.5.0, 1.4.1, and 1.3.1.

Score Functions:
* [field_value_factor_with_default](docs/field_value_factor_with_default.md) -
Just like field_value_factor except it supports a ```missing``` parameter that
is the value used if the field is missing from the document being scored. Added
in 1.5.0, 1.4.1, and 1.3.0.

Installation
------------

| Extra Queries and Filters Plugin |  ElasticSearch  |
|----------------------------------|-----------------|
| 1.5.0, master branch             | 1.5.X           |
| 1.4.0 -> 1.4.1, 1.4 branch       | 1.4.X           |
| 1.3.0 -> 1.3.1, 1.3 branch       | 1.3.4 -> 1.3.X  |
| 0.0.1 -> 0.0.2                   | 1.3.2 -> 1.3.3  |

Install it like so for Elasticsearch 1.5.x:
```bash
./bin/plugin --install org.wikimedia.search/extra/1.5.0
```

Install it like so for Elasticsearch 1.4.x:
```bash
./bin/plugin --install org.wikimedia.search/extra/1.4.1
```

and for Elasticsearch 1.3.x:
```bash
./bin/plugin --install org.wikimedia.search/extra/1.3.1
```
