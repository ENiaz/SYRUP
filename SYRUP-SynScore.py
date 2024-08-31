import json
import csv
import operator
import itertools
from SPARQLWrapper import SPARQLWrapper, JSON
import time
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"
from pandasql import sqldf


import Metric
import warnings



class Predicate():
    def construct(self, dict_pre_label):
        self.pre_label = dict_pre_label
    
    def __init__(self):
        self.pre_label = dict()

predicate = Predicate()




def current_milli_time():
    return round(time.time() * 1000)



def inputFile(input_config):
    with open(input_config, "r") as input_file_descriptor:
        input_data = json.load(input_file_descriptor)
    prefix = input_data['prefix']
    path = './'+ input_data['KG']
    rules = path + '/Metric/'+ input_data['rules_file']
    endpoint = input_data['endpoint']
    path_result = 'Results/' + input_data['KG'] + "_NewTriples/" + ".nt"
    predicate = input_data['predicate']

    return endpoint, prefix, rules, path, predicate, path_result 


# Extract the predicate in body with 2 atoms where the PCA_Confidence is high
def colectRules(file, user_predicate):
    global first_preAtom, second_preAtom, final_preds
    rules = pd.read_csv(file)
    q = f"""SELECT * FROM rules WHERE Body LIKE '%?%  %  ?%  ?%  %  ?%   %' AND Head LIKE '%{user_predicate}%' ORDER BY PCA_Confidence DESC"""

    rule = sqldf(q, locals())
    final_preds = []

    for idx, item in rule.iterrows():
        sub_dataframe = pd.DataFrame([item])
        
        for i, val in sub_dataframe.iterrows():
            body = val['Body']
            fun_var = val['Functional_variable']
            preds = body.split()
            pattern = re.compile(r'^\w+$')
            top_list = [item if pattern.match(item) else item for item in preds]

     
            split_index = 3
            first_preAtom = top_list[:split_index]
            second_preAtom = top_list[split_index:]

            first_preBody = ' '.join(first_preAtom)
            second_preBody = ' '.join(second_preAtom)
            

            
        final_preds.append(first_preBody)
        final_preds.append(second_preBody)
    body1 = []
    body2 = []   
    # print(final_preds)
    for i,k in zip(final_preds[0::2], final_preds[1::2]):
        body1.append(str(i))
        body2.append(str(k))  
    
    return body1, body2
            

def query_generationMdifiedPCA(endpoint, prefix, pre1, pre2, pre3):
    prex1 = []
    pattern = r'\?([A-Za-z])'
    for item in pre2:
        new_item = re.sub(pattern, r'?\g<1>1', item, count=1)
        prex1.append(new_item)
    print(prex1)
    
    sparql_query_template = """
        PREFIX ex: <http://family.org/>
        SELECT (xsd:float(?Support))/MAX(?PCA) AS ?modifiedPCA WHERE {
          {
            SELECT (COUNT(DISTINCT *) AS ?Support) WHERE {
              SELECT ?a ?b WHERE {body_var2.
              body_var1.
              ?a head_var ?b.}
            }
          }
          {
            {SELECT (COUNT(DISTINCT *) AS ?PCA) WHERE {
              SELECT ?a ?b WHERE {body_var2.
              body_var1.
              ?a head_var ?b1.}}
            }
          }
          UNION
          {
            {SELECT (COUNT(DISTINCT *) AS ?PCA) WHERE {
              SELECT ?a ?b WHERE {body_var2.
              body_varX1.
              ?a head_var ?b.}}
            }
          }
        }
        """
    
    if len(pre2) != len(pre3):
        print("Both lists should have the same length.")
        return
    
    modifiedPCAs = []  # Store the modifiedPCA values for all queries
    
    for elem2, elemx1, elem3 in zip(pre2, prex1, pre3):
        sparql_query = sparql_query_template.replace("head_var", pre1).replace("body_var1", elem2).replace("body_varX1", elemx1).replace("body_var2", elem3)

        # print(sparql_query)
        
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)

        # Execute the query and print the results
        results = sparql.query().convert()
        
        for result in results["results"]["bindings"]:
            modifiedPCA = result["modifiedPCA"]["value"]
            print(f"The value of ModifiedPCA for predicateBody={elem2} and predicateBody={elem3}: {modifiedPCA}")
            modifiedPCAs.append(modifiedPCA)

    return modifiedPCA
        
        




if __name__ == '__main__':
    start_time = time.time()
    input_config = 'input-SynScore.json'
    endpoint, prefix, rulesfile, path, predicate, path_result = inputFile(input_config)
    preB1, preB2 = colectRules(rulesfile, predicate)
    score = query_generationMdifiedPCA(endpoint, prefix, predicate, preB1, preB2)
    

    end_time = time.time()
    execution_time = end_time - start_time

    print('Execution time', execution_time)



