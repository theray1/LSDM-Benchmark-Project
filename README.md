# Projet Large Scale Data Management
## GOAL

The goal of the project is to run benchmarks regarding the execution time of the PageRank algorithm depending of several parameters.

## CONTRIBUTORS

 - Bilal MOLLI

 - Erwan BOISTEAU-DESDEVISES

## EXPERIMENTS

We ran experiments with every combination of the following parameters :

1. Algorithm 
    - PIG
    - Spark w/ data localization
    - Spark w/o data localization

2. Number of worker nodes
    - 2
    - 3
    - 4

Every bucket / worker node was situated in the same geographical zone (EUW6) to avoid network latency.

The dataset which the experiments were ran on is located at gs://public_lddm_data/page_links_en.nt.bz2.

The output ranking gives us <http://dbpedia.org/resource/Living_people> as the top ranked page, with a score of 36794.

## RESULTS

![Results](plotpagerank.PNG)

We can see that Spark is always faster with localized data, and that regardless of data localization, Spark is faster than PIG likely due to the fact to Spark doesn't need to write to disk nearly as much as PIG.

Furthermore, as expected, the execution time of every algorithm decreases as we increase the number of worker node.
