#!/usr/bin/env python
# coding: utf-8

# First, the requirements are installed, the code of SAP-KG is imported, and some reoccurring variables are set:



# pip install --no-cache-dir -r requirements.txt



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



# df_pre = pd.read_csv('Predicates.csv')
# predicate_list = list(df_pre.Label.unique())
# dict_predicate_label = dict(zip(df_pre.Label, df_pre.Predicate))
# # print(dict_predicate_label)
# predicate.construct(dict_predicate_label)



def current_milli_time():
    return round(time.time() * 1000)



def inputFile(input_config):
    with open(input_config, "r") as input_file_descriptor:
        input_data = json.load(input_file_descriptor)
    prefix = input_data['prefix']
    path = './'+ input_data['KG']
    rules = path + '/Metric/'+ input_data['rules_file']
    query_path = path + '/Queries/' + input_data['domain'] + "_Domain/" + input_data['query'] + ".txt"
    endpoint = input_data['endpoint']
    path_result = 'Results/' + input_data['domain'] + "_Domain/" + "rewritten_" + input_data['query'] + ".txt"
    

    return endpoint, prefix, rules, path, query_path, path_result 

def read_query_predicate(query):
    with open(query, 'rt') as f:
        query_user = f.read()
        # print(query_user)
        input_string = re.findall('<.*>', query_user)[-1][1:-1]
        last_slash_index = input_string.rfind('/')

        if last_slash_index != -1:
            predicate_check = input_string[last_slash_index + 1:]       

    return predicate_check

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
            
            preds = body.split()
            pattern = re.compile(r'^\w+$')
            top_list = [item if pattern.match(item) else item for item in preds]

     
            split_index = 3
            first_preAtom = top_list[:split_index][1]
            second_preAtom = top_list[split_index:][1]
            
        final_preds.append(first_preAtom)
        final_preds.append(second_preAtom)


    return final_preds[0], final_preds[1]

            
def replace_rule_instances(file, query, user_predicate):
    global first_preAtom, second_preAtom, final_preds
    rules = pd.read_csv(file)
    q = f"""SELECT * FROM rules WHERE Body LIKE '%?%  %  ?%  ?%  %  ?%   %' AND Head LIKE '%{user_predicate}%' ORDER BY PCA_Confidence DESC"""
   
    rule = sqldf(q, locals())
    final_preds = []

    for idx, item in rule.iterrows():
        sub_dataframe = pd.DataFrame([item])
        
        for i, val in sub_dataframe.iterrows():
            body = val['Body']
            headAtom = val['Head']
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
    firstAtom = []
    secondAtom = []
    for i,k in zip(final_preds[0::2], final_preds[1::2]):
        firstAtom.append(str(i))
        secondAtom.append(str(k))  
   
    with open(query, 'r') as file:
        lines = file.read()
        firstVar = re.findall('.*<', lines)[-1][0:-1]
        secondVar = re.findall('>.*', lines)[-1][2:-1]

    firstAtoms = [string.replace('?a', firstVar).replace('?b', secondVar) for string in firstAtom]
    secondAtoms = [string.replace('?a', firstVar).replace('?b', secondVar) for string in secondAtom]
    headAtom = headAtom.replace('?a', firstVar).replace('?b', secondVar)

    return firstAtoms[0], secondAtoms[0], headAtom

# https://labs.tib.eu/sdm/wikidata-09-23/sparql
# https://labs.tib.eu/sdm/dbpedia/sparql
# ?b  parent  ?f  ?f  spouse  ?a   ,?a  child  ?b

# compute PCA

# PREFIX ex: <http://dbpedia.org/ontology/>
# SELECT (xsd:float(?Support)/xsd:float(?PCABodySize) AS ?PCA) 
# WHERE {
# {SELECT (COUNT(DISTINCT *) AS ?Support) WHERE {
#                   SELECT ?X ?Y  WHERE {
#                                  ?F ex:spouse ?X.
#                                  ?Y ex:parent ?F.
#                                  ?X ex:child ?Y. } }} 
# {SELECT (COUNT(DISTINCT *) AS ?PCABodySize) WHERE {
#                   SELECT ?X ?Y  WHERE {
#                                     ?F ex:spouse ?X.                                   
#                                     ?Y ex:parent ?F.
#                                     ?X ex:child ?Y1.}}}}


# compute Modified-PCA

# PREFIX ex: <http://dbpedia.org/ontology/>
# SELECT (xsd:float(?Support))/MAX(?PCA) AS ?modifiedPCA WHERE {{
# SELECT (COUNT(DISTINCT *) AS ?Support) WHERE { 
# SELECT ?X ?Y WHERE { ?F ex:spouse ?X. ?Y ex:parent ?F. ?X ex:child ?Y. }}} 
# { {SELECT (COUNT(DISTINCT *) AS ?PCA) WHERE { 
# SELECT ?X ?Y WHERE { ?F ex:spouse ?X. ?Y ex:parent ?F. ?X ex:child ?Y1. }}}}
# UNION 
# { {SELECT (COUNT(DISTINCT *) AS ?PCA) WHERE { 
# SELECT ?X ?Y WHERE { ?F ex:spouse ?X. ?Y1 ex:parent ?F. ?X ex:child ?Y. }}}
# }}

def query_generationMdifiedPCA(endpoint, prefix, pre1, pre2, pre3):
    pattern = r'\?([a-zA-Z])1'
    elemx1 = re.sub(pattern, r'?\g<1>11', pre2)
       
    # Define SPARQL query
    sparql_query_template = """
    PREFIX ex: <""" + prefix + """>
    SELECT (xsd:float(?Support))/MAX(?PCA) AS ?modifiedPCA WHERE {
      {
        SELECT (COUNT(DISTINCT *) AS ?Support) WHERE {
          SELECT ?s1 ?o1 WHERE {body_var2.
          body_var1.
          ?s1 ex:head_var ?o1.}
        }
      }
      {
        {SELECT (COUNT(DISTINCT *) AS ?PCA) WHERE {
          SELECT ?s1 ?o1 WHERE {body_var2.
          body_var1.
          ?s1 ex:head_var ?o11.}}
        }
      }
      UNION
      {
        {SELECT (COUNT(DISTINCT *) AS ?PCA) WHERE {
          SELECT ?s1 ?o1 WHERE {body_var2.
          body_varX1.
          ?s1 ex:head_var ?o1.}}
        }
      }
    }
    """

    
    # Set the variable values to bind to the placeholders
    # Replace the placeholders with the variable values
    sparql_query = sparql_query_template.replace("head_var", pre1).replace("body_var1", pre2).replace("body_varX1", elemx1).replace("body_var2", pre3)
    print(sparql_query)
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(sparql_query)
    sparql.setReturnFormat(JSON)

    # Execute the query and print the results
    results = sparql.query().convert()
    # Extract and print the values
    for result in results["results"]["bindings"]:
        modifiedPCA = result["modifiedPCA"]["value"]
        print("The value of ModifiedPCA:", modifiedPCA)

    return modifiedPCA





# for Numeric-Embedding technique
# def similarity_calculation(vectors):
#     vec = np.asarray(vectors)
#     vec_normalized = np.zeros(vec.shape)
#     for i in range(vec.shape[0]):
#         vec_normalized[i] = vec[i] / np.sqrt(vec[i] @ vec[i].T)
#     sim_mat = np.abs(vec_normalized @ vec_normalized.T)

#     return sim_mat.tolist()


# predicates are list of strings
# def sim_mat_to_csv(sim_mat, predicates):
#     sim_mat = np.asarray(sim_mat)
#     tabular = np.empty((sim_mat.shape[0], sim_mat.shape[1] + 1), dtype=object)
#     for i in range(sim_mat.shape[0]):
#         tabular[i][0] = predicates[i]
#         for j in range(1, sim_mat.shape[1] + 1):
#             tabular[i][j] = predicates[j - 1] + '(' + str(sim_mat[i][j - 1]) + ')'
#     tabular = pd.DataFrame(tabular)

#     return tabular.to_csv('Results/Person_Synonyms/Synonyms.csv')

def highest_similar_predicates(prefix, pre1, num=4):
    with open('Results/Person_Synonyms/Synonyms.csv', 'r') as synfile:
        synonymVal = csv.reader(synfile)
        extracted_values_list = []
        for row in synonymVal:
            # Initialize a list to store extracted values for the current row
            row_values = []
            for element in row:
                valuesSyn = re.findall(r'\((.*?)\)', element)
                row_values.extend(valuesSyn)

            extracted_values_list.append(row_values)

        cleaned_list = extracted_values_list[1:]
        sim_mat = np.array(cleaned_list)
        sim_table = pd.read_csv('Results/Person_Synonyms/Synonyms.csv')
        pred = prefix + pre1
        row_num = sim_table[sim_table['0'] == pred].index.values[0]
        row = sim_mat[row_num, :]
        row_dict = {}
        for e, v in zip(sim_table['0'].values, row):
            row_dict[e] = v
        predicates = list({k: v for k, v in sorted(row_dict.items(), key=lambda item: item[1], reverse=True)[1:num+1]}.keys())
        return predicates 
        




def query_expansion2(query, predicate_to_replace, atom1, atom2):
    if type(query) is not str:
        raise TypeError('query must be string!')
    
    if predicate_to_replace[0] != '<':
        predicate_to_replace = '<' + predicate_to_replace + '>'

    new_query = query
    
    atoms = []
    atoms.append(atom1Pref)
    atoms.append(atom2Pref)

    part_within_braces = re.findall('\{.*\}', query, flags=re.DOTALL)[0]
    
    firstVar = re.findall('.*<', query)[-1][0:-1]
    secondVar = re.findall('>.*', query)[-1][2:-1]
    
    # predicate_to_replace = firstVar + ' ' + predicate_to_replace + ' ' + secondVar
    predicate_to_replace = '?s1 '+ predicate_to_replace + ' ?o1'
    
    for predicate in atoms:
        new_query = new_query + '\nunion\n' + part_within_braces.replace(predicate_to_replace, predicate)
    slice_point = [match for match in re.finditer('\{', new_query)][0].start()-1
    
    new_query = new_query[:slice_point] + '{' + new_query[slice_point:] + '}'
    

    return new_query

if __name__ == '__main__':
    start_time = time.time()
    input_config = 'input.json'
    endpoint, prefix, rulesfile, path, query, path_result = inputFile(input_config)
    userquery = read_query_predicate(query)
    # sim_mat = similarity_calculation(vectors)
    # sim_mat_to_csv(sim_mat, predicates)
    preB1, preB2 = colectRules(rulesfile, userquery)
    
    atom1, atom2, atomHead = replace_rule_instances(rulesfile, query, userquery)
    query_generationMdifiedPCA(endpoint, prefix, userquery, atom1, atom2)
    
    with open(query, 'rt') as f:
        query = f.read()
    add_synonym_predicates = highest_similar_predicates(prefix, userquery)
    userqueryPref = prefix + userquery
    atom1Pref = atom1.replace(preB1, f'<{prefix}{preB1}>').replace('ex:', '')
    atom2Pref = atom2.replace(preB2, f'<{prefix}{preB2}>').replace('ex:', '')
    
    
    
    # a = query_expansion(query, userqueryPref, add_synonym_predicates)
    aa = query_expansion2(query, userqueryPref, atom1Pref, atom2Pref)
    with open(path_result , 'wt') as f:
        f.write(aa)

    end_time = time.time()
    execution_time = end_time - start_time

    print('Execution time', execution_time)


def calc_true_positives(classified_synonyms_set, ground_truth_synonyms_set):
    return set(filter(lambda x: x in ground_truth_synonyms_set, classified_synonyms_set))
    
def calc_precision_recall(classified_synonyms_set, ground_truth_synonyms_set):
    num_true_positives = len(calc_true_positives(classified_synonyms_set, ground_truth_synonyms_set))
    num_classified_positives = len(classified_synonyms_set)
    num_ground_truth_positives = len(ground_truth_synonyms_set)
    precision = (float(num_true_positives) / float(num_classified_positives)) if num_classified_positives != 0 else 0.0
    recall = (float(num_true_positives) / float(num_ground_truth_positives)) if num_ground_truth_positives != 0 else 0.0
    return precision, recall






def NumberAnswers(NumberAnswers):
# Read the CSV files into pandas DataFrames
    df1 = pd.read_csv('experiments/DBpedia_rewriting/NumberQueriesOriginal.csv')  
    df2 = pd.read_csv('experiments/DBpedia_rewriting/NumberQueriesSYRUPdiff.csv')  

    
    columns_to_plot = ['Film', 'Sport', 'Person', 'Drug', 'Music', 'History']  

    # Iterate over each pair of columns and create a plot
    for column in columns_to_plot:
        df1[column] = df1[column].astype(float)
        df2[column] = df2[column].astype(float)

        df1[column].fillna(0, inplace=True)
        df2[column].fillna(0, inplace=True)

        # Normalize the data
        total = df1[column] + df2[column]


        df1['Normalized'] = df1[column] / total
        df2['Normalized'] = df2[column] / total

        
        plt.figure(figsize=(8, 6))
        plt.bar(df1['Queries'], df1['Normalized'], label='Original Answers', alpha=0.7)
        plt.bar(df1['Queries'], df2['Normalized'], bottom=df1['Normalized'], label='SYRUP Answers', alpha=0.7)

        
        plt.xlabel('Queries', fontsize=20)
        plt.ylabel('#Normalized Answers', fontsize=20)
        # plt.title(f'Stacked Bar Plot for {column} (Normalized)')
        plt.legend(fontsize=20)

        
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'experiments/stacked_bar_{column}.png')

NumberAnswers(NumberAnswers) 



