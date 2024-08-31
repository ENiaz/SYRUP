import os
import glob
import csv
from rdflib import Graph, URIRef
from SPARQLWrapper import SPARQLWrapper, JSON


sparql_endpoint = 'https://dbpedia.org/sparql'

def run_sparql_query(query_str):
    sparql = SPARQLWrapper(sparql_endpoint)

 
    sparql.setQuery(query_str)


    sparql.setReturnFormat(JSON)

   
    results = sparql.query().convert()
    
    return results

def process_queries_in_directory(query_directory):

    query_files = glob.glob(os.path.join(query_directory, '*.txt'))

    if not query_files:
        print(f"No query files found in the directory '{query_directory}'.")
        return

    query_results = []

    for query_file in query_files:
        with open(query_file, 'r') as file:
            query_str = file.read()

        # Run the SPARQL query
        print(f"Running query from file '{query_file}':\n{query_str}\n")
        results = run_sparql_query(query_str)

        # Process and accumulate the query results
        if 'results' in results:
            bindings = results['results']['bindings']
            if bindings:
                print("Query results:")
                for binding in bindings:
                    query_results.append([query_file, binding])
                    for var_name, value in binding.items():
                        print(f"{var_name}: {value['value']}")
                    print("\n")
            else:
                print("No results found for this query.\n")
        else:
            print("Error in query execution:\n", results, "\n")

    return query_results


def main():
    query_directories = ['DBpedia/Queries/Drug_Domain', 'DBpedia/Queries/Person_Domain', 'DBpedia/Queries/Sport_Domain', 'DBpedia/Queries/Music_Domain', 'DBpedia/Queries/History_Domain', 'DBpedia/Queries/Film_Domain']
    # query_directories = ['DBpedia/Gold_Standard/Drug_Domain', 'DBpedia/Gold_Standard/Person_Domain', 'DBpedia/Gold_Standard/Sport_Domain', 'DBpedia/Gold_Standard/Music_Domain', 'DBpedia/Gold_Standard/History_Domain', 'DBpedia/Gold_Standard/Film_Domain']
    # query_directories = ['Results/Drug_Domain', 'Results/Person_Domain', 'Results/Sport_Domain', 'Results/Music_Domain', 'Results/History_Domain', 'Results/Film_Domain']
    output_csv_file = 'experiments/DBpedia_rewriting/query_results.csv' 

    all_query_results = []

    for query_directory in query_directories:
        query_results = process_queries_in_directory(query_directory)
        all_query_results.extend(query_results)

    # Write all query results to the output CSV file
    with open(output_csv_file, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["Query File", "Variable Name", "Value"])
        
        for query_result in all_query_results:
            query_file = query_result[0]
            binding = query_result[1]
            
            for var_name, value in binding.items():
                csv_writer.writerow([query_file, var_name, value['value']])

if __name__ == "__main__":
    main()
