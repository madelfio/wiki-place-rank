A project for computing the PageRank of Wikipedia pages for places.

Specifically, a current Wikipedia XML dump is used to generate the
Wikipedia link graph.  The Geonames RDF dump is used for a mapping
between Geonames entities and Wikipedia pages.  The resulting
collection of ranked locations can be used for estimating the
prominence of each interpretation of a toponym.

To run, first download:

- A dump of the [English Language Wikipedia][1]

- The Geonames [RDF Dump][2]

[1]: http://en.wikipedia.org/wiki/Wikipedia:Database_download#English-language_Wikipedia
[2]: http://www.geonames.org/ontology/documentation.html

Install this package:

```go get github.com/madelfio/wiki-place-rank/```

Run the following:

1) `wiki-place-rank graph $WIKIDUMP 1-page-graph.gob`

2) `wiki-place-rank pagerank 1-page-graph.gob 2-page-rank.gob`

3) `wiki-place-rank locations $GEONAMESDUMP 3-locations.gob`

4) `wiki-place-rank georank 2-page-rank.gob 3-locations.gob 4-geo-ranks.txt`

Or, as one command (intermediate files are stored on /tmp):

- `wiki-place-rank all $WIKIDUMP $GEONAMESDUMP geo-ranks.txt`
