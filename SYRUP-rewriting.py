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
    q = f"""SELECT * FROM rules WHERE Body LIKE '%?%  %  ?%  ?%  %  %   %' AND Head LIKE '%{user_predicate}%' ORDER BY PCA_Confidence DESC"""

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

            
def replace_rule_instances(file, user_predicate):
    global first_preAtom, second_preAtom, final_preds
    rules = pd.read_csv(file)
    q = f"""SELECT * FROM rules WHERE Body LIKE '%?%  %  ?%  ?%  %  %   %' AND Head LIKE '%{user_predicate}%' ORDER BY PCA_Confidence DESC"""

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



def query_rewriting(query, predicate_to_replace, atom1, atom2):
    if type(query) is not str:
        raise TypeError('query must be string!')
    
    if predicate_to_replace[0] != '<':
        predicate_to_replace = '<' + predicate_to_replace + '>'

    new_query = query
    predicates = ''
    atoms = []
    atoms.append(atom1Pref)
    atoms.append(atom2Pref)
    part_within_braces = re.findall('\{.*\}', query, flags=re.DOTALL)[0]
    firstVar = re.findall('.*<', query)[-1][0:-1]
    secondVar = re.findall('>.*', query)[-1][2:-1]
    
    # predicate_to_replace = firstVar + ' ' + predicate_to_replace + ' ' + secondVar
    predicate_to_replace = '?s1 '+ predicate_to_replace + ' ?o1'
    
    for index, predicate in enumerate(atoms):
        if index == 0:
            predicate += '.\n'
        if index > 0:
            predicates += ' '

        predicates += predicate
    
    new_query = new_query + '\nunion\n' + part_within_braces.replace(predicate_to_replace, predicates)
    slice_point = [match for match in re.finditer('\{', new_query)][0].start()-1
    
    new_query = new_query[:slice_point] + '{' + new_query[slice_point:] + '}'
    

    return new_query

if __name__ == '__main__':
    start_time = time.time()
    input_config = 'input-rewriting.json'
    endpoint, prefix, rulesfile, path, query, path_result = inputFile(input_config)
    userquery = read_query_predicate(query)
    preB1, preB2 = colectRules(rulesfile, userquery)
    
    atom1, atom2, atomHead = replace_rule_instances(rulesfile, userquery)

    with open(query, 'rt') as f:
        query = f.read()
    
    userqueryPref = prefix + userquery
    atom1Pref = atom1.replace(preB1, f'<{prefix}{preB1}>').replace('ex:', '')
    atom2Pref = atom2.replace(preB2, f'<{prefix}{preB2}>').replace('ex:', '')

    # a = query_expansion(query, userqueryPref, add_synonym_predicates)
    aa = query_rewriting(query, userqueryPref, atom1Pref, atom2Pref)
    with open(path_result , 'wt') as f:
        f.write(aa)

    end_time = time.time()
    execution_time = end_time - start_time

    print('Execution time', execution_time)








