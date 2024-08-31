import json
import re
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from pandasql import sqldf
from IPython.core.interactiveshell import InteractiveShell

InteractiveShell.ast_node_interactivity = "all"

def inputFile(input_config):
    with open(input_config, "r") as input_file_descriptor:
        input_data = json.load(input_file_descriptor)
    prefix = input_data['prefix']
    path = './' + input_data['KG']
    rules = path + '/Metric/' + input_data['rules_file']
    query_path = path + '/Queries/' + input_data['domain'] + "_Domain/" + input_data['query'] + ".txt"
    endpoint = input_data['endpoint']
    path_result = 'Results/' + input_data['domain'] + "_Domain/" + "rewritten_" + input_data['query'] + ".txt"
    predicateHead = input_data['predicateHead']

    return endpoint, prefix, rules, path, query_path, path_result, predicateHead

def colectRules(file, predicateHead):
    global first_preAtom, second_preAtom, final_preds
    rules = pd.read_csv(file)
    q = f"""SELECT * FROM rules WHERE Body LIKE '%?%  %  ?%  ?%  %  ?%   %' AND Head LIKE '%{predicateHead}%' ORDER BY PCA_Confidence DESC"""
   
    rule = sqldf(q, locals())
    print(rule)
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

def replace_rule_instances(file, query, predicateHead):
    rules = pd.read_csv(file)
    q = f"""SELECT * FROM rules WHERE Body LIKE '%?%  %  ?%  ?%  %  ?%   %' AND Head LIKE '%{predicateHead}%' ORDER BY PCA_Confidence DESC"""
   
    rule = sqldf(q, locals())
    rule_bodies = []
    rule_heads = []

    for idx, item in rule.iterrows():
        sub_dataframe = pd.DataFrame([item])
        for i, val in sub_dataframe.iterrows():
            body = val['Body']
            head = val['Head']
            preds = body.split()
            pattern = re.compile(r'^\w+$')
            top_list = [item if pattern.match(item) else item for item in preds]

            split_index = 3
            first_preAtom = ' '.join(top_list[:split_index])
            second_preAtom = ' '.join(top_list[split_index:])

            rule_body = f"{first_preAtom}. {second_preAtom}"
            rule_bodies.append(rule_body)
            rule_heads.append(head)
    
    with open(query, 'r') as file:
        lines = file.read()
        firstVar = re.findall('.*<', lines)[-1][0:-1]
        secondVar = re.findall('>.*', lines)[-1][2:-1]

    rule_bodies = [body.replace('?a', firstVar).replace('?b', secondVar) for body in rule_bodies]
    rule_heads = [head.replace('?a', firstVar).replace('?b', secondVar) for head in rule_heads]
    
    return rule_bodies, rule_heads

def compare_sparql_results(head1, rule_bodies):
    
    sparql_endpoint = "http://dbpedia.org/sparql"

    
    def execute_query(sparql_endpoint, query):
        sparql = SPARQLWrapper(sparql_endpoint)
        sparql.setQuery(query)
        print(query)  # Print query for debugging
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        return int(results["results"]["bindings"][0]["comp"]["value"])

    for i in range(2, len(rule_bodies) + 1):
        atom1 = rule_bodies[i-2]  # Update atom1 with each new body
        query1 = f"""
        PREFIX ex: <http://dbpedia.org/ontology/>
        SELECT MAX(?instances) AS ?comp WHERE {{
          {{
            SELECT (COUNT(DISTINCT ?o1) AS ?instances) WHERE {{
              SELECT ?o1 WHERE {{
                {atom1}
              }}
            }}
          }}
          UNION
          {{
            SELECT (COUNT(DISTINCT ?o1) AS ?instances) WHERE {{
              SELECT ?o1 WHERE {{
                {head1}
              }}
            }}
          }}
        }}
        """
        
        union_clauses = ' UNION '.join([f"""
        {{
          SELECT (COUNT(DISTINCT ?o1) AS ?instances) WHERE {{
            SELECT ?o1 WHERE {{
              {body}
            }}
          }}
        }}
        """ for body in rule_bodies[:i]])
        
        extracted_predicate_pairs = set()
        for body in rule_bodies[:i]:
            pairs = re.findall(r'\?[\w\d]+\s+(ex:\w+)\s+\?[\w\d]+', body)
            if len(pairs) >= 2:
                predicate_pair = (pairs[0], pairs[1])
                extracted_predicate_pairs.add(predicate_pair)

        # Create formatted string with the extracted predicate pairs
        predicates_string = "{{" + "},{".join(["" + ", ".join(pair) + "" for pair in extracted_predicate_pairs]) + "}}"
        print("Minimal Set of ADs:", predicates_string)

        query2 = f"""
        PREFIX ex: <http://dbpedia.org/ontology/>
        SELECT MAX(?instances) AS ?comp WHERE {{
          
          {union_clauses}
        }}
        """
        
        # Execute the query and compare the results
        result1 = execute_query(sparql_endpoint, query1)
        result2 = execute_query(sparql_endpoint, query2)
        print(f"Result1: {result1}, Result2: {result2}")

        # Compare the results and print "terminate" if result1 < result2
        if result1 < result2:
            print("terminate")
            break
        else:
            print("continue")

    # print("Minimal Set of ADs:", )
# Call the function to perform the comparison
if __name__ == '__main__':
    # input_config = 'input.json'
    input_config = 'input.json'
    endpoint, prefix, rulesfile, path, query, path_result, mainPredicate = inputFile(input_config)
    preB1, preB2 = colectRules(rulesfile, mainPredicate)
    rule_bodies, rule_heads = replace_rule_instances(rulesfile, query, mainPredicate)
    print(rule_bodies)
    print(rule_heads)
    # Pass the first rule head and rule bodies to compare_sparql_results as an example
    if len(rule_bodies) >= 2:
        compare_sparql_results(rule_heads[0], rule_bodies)

