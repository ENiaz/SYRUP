import json
import operator
import itertools
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import time

KGDBpedia = 'https://dbpedia.org/sparql'
KGWikidata = 'https://query.wikidata.org/sparql'


def current_milli_time():
    return round(time.time() * 1000)



#Cardinality based on triple Wikidata
def query_generationWikidata1(pre1):
    query_select_clause = "SELECT (COUNT(?p) as ?count)"
    if """dbpedia""" in pre1:
       query_where_clause = """WHERE { ?s ?p ?o. ?s """
       query_where_clause = query_where_clause + "<" + pre1 + "> ?o" + ".\n"
       query_where_clause = query_where_clause + """FILTER (str(?p) IN (\"""" + pre1 + "\")) .}"""
       sparqlQuery = query_select_clause + " " + query_where_clause
       # print(sparqlQuery)
       sparql = SPARQLWrapper(KGDBpedia)
       sparql.setQuery(sparqlQuery)
       sparql.setReturnFormat(JSON)
       results = sparql.query().convert()
       data = results["results"]["bindings"]

       print("Cardinality of triples with Predicate 1:", int(data[0]["count"]["value"]))
       return int(data[0]["count"]["value"])
    else:
       query_where_clause = """WHERE { ?s ?p ?o. """
       query_where_clause = query_where_clause + """FILTER (?p IN( """ + pre1 + ")) ."""
       query_where_clause = query_where_clause[:-1] + "}"
       sparqlQuery = query_select_clause + " " + query_where_clause
       # print(sparqlQuery)
       sparql = SPARQLWrapper(KGWikidata)
       sparql.setQuery(sparqlQuery)
       sparql.setReturnFormat(JSON)
       results = sparql.query().convert()
       data = results["results"]["bindings"]
       print("Cardinality of triples with Predicate 1:",int(data[0]["count"]["value"]))
       return int(data[0]["count"]["value"])

#Cardinality based on triple Wikidata
def query_generationWikidata2(pre2):
    query_select_clause = "SELECT (COUNT(?p) as ?count)"
    if """dbpedia""" in pre2:
       query_where_clause = """WHERE { ?s ?p ?o. ?s """
       query_where_clause = query_where_clause + "<" + pre2 + "> ?o" + ".\n"
       query_where_clause = query_where_clause + """FILTER (str(?p) IN (\"""" + pre2 + "\")) .}"""
       sparqlQuery = query_select_clause + " " + query_where_clause
       # print(sparqlQuery)
       sparql = SPARQLWrapper(KGDBpedia)
       sparql.setQuery(sparqlQuery)
       sparql.setReturnFormat(JSON)
       results = sparql.query().convert()
       data = results["results"]["bindings"]
       print("Cardinality of triples with Predicate 2:", int(data[0]["count"]["value"]))
       return int(data[0]["count"]["value"])
    else:    
       query_where_clause = """WHERE { ?s ?p ?o. """
       query_where_clause = query_where_clause + """FILTER (?p IN( """ + pre2 + ")) ."""
       query_where_clause = query_where_clause[:-1] + "}"
       sparqlQuery = query_select_clause + " " + query_where_clause
       # print(sparqlQuery)
       sparql = SPARQLWrapper(KGWikidata)
       sparql.setQuery(sparqlQuery)
       sparql.setReturnFormat(JSON)
       results = sparql.query().convert()
       data = results["results"]["bindings"]
       print("Cardinality of triples with Predicate 2:", int(data[0]["count"]["value"]))
       return int(data[0]["count"]["value"])


def computeMetricOverlap(pre2, pre1):
    #input_cui = pre2 + pre1
    random_id = str(current_milli_time())
    # with open(input_file, "r") as input_file_descriptor:
    #     input_data = json.load(input_file_descriptor)

    countWikidata1 = query_generationWikidata1(pre1)
    countWikidata2 = query_generationWikidata2(pre2)
       
    metric = round(float(min(countWikidata1, countWikidata2) / max(countWikidata1, countWikidata2)) * 100, 2)
    print("Percentage of Total Overlap-Synonym:", metric)
    return metric

def load_data(file):
    pre2 = file["Input"]["IndependentVariables"]["WikidataPredicate2"]
    pre1 = file["Input"]["IndependentVariables"]["WikidataPredicate"]

    return computeMetricOverlap(pre2, pre1)

# if __name__ == '__main__':
#     res = computeMetricOverlap("inputfilePredicateInter.json")