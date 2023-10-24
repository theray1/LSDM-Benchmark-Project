import findspark
import os
from operator import add
import re

findspark.init()
from pyspark.sql import SparkSession


spark = SparkSession.builder\
        .master("local"w+')\
        .appName("Colab")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()

lines = spark.read.text("small_page_links.nt").rdd.map(lambda r: r[0])

def computeContribs(urls, rank) :
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls) :
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[2]

# Loads all URLs from input file and initialize their neighbors.
links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

# Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))


for iteration in range(1):
    links = links.partitionBy(links.getNumPartitions())
    ranks = ranks.partitionBy(ranks.getNumPartitions())
  # Calculates URL contributions to the rank of other URLs.
    contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1]  # type: ignore[arg-type]
        ))
  # Re-calculates URL ranks based on neighbor contributions.
    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
try:
  os.remove("/text")
except:
  pass
ranks.saveAsTextFile('/text')