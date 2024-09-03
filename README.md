[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
## Completing Predicates based on Alignment Rules from Knowledge Graphs

SYRUP is a two-fold approach to detect a minimal number of alternative definitions based on alignment rules; it defines a metric to measure whether detected alternative definitions are complementary. These methods are implemented in the engine SYRUP. There are many techniques to detect the unknown positive facts in KGs, including the utilization of embedding-based methods in link prediction. The experimental results depict that detecting a minimal set of alternative definitions based on alignment rules are capable of competing with the state-of-the-art embedding-based techniques. To show the effectiveness of SYRUP, the discovered alternative definitions for predicates are used in query expansion to retrieve maximum answers. 

![SYRUP example](/images/MotivatingExample.png?raw=true "SYRUP example")





### Building SYRUP from Source
Clone the repository
```git
git clone git@github.com:SDM-TIB/SYRUP.git
```
```python
pip install -r requirements.txt
```
Configuration for executing
```json
{
  "KG": "DBpedia",
  "endpoint": "https://labs.tib.eu/sdm/dbpedia/sparql",
  "prefix": "http://dbpedia.org/ontology/",
  "rules_file": "DBpedia_AMIE_Rules.csv",
  "domain": "Person",
  "query": "Q1",
  "predicateHead": "child"
}
```

The proposed approach is a a knowledge graph-agnostic approach. Therefore, apart from DBpedia, other KGs, i.e., ```FrenchRoyalty``` or ```Family``` can be used as the parameter ``KG``.

```python
python SYRUP.py 
```
### Plots demonstrating
```python
python -m plot_evaluation -e experiments/DBpedia
```

![SYRUP evaluation](/images/PrecisionRecallEval.png?raw=true "SYRUP evaluation")
