# Importing necessary modules
import sys
from run_cypher_queries import check_connection_to_graph_db, run_list_of_cypher_queries


def remove_all(bolt_url, user_name, password):
    rm_properties_query = "MATCH (n) SET n = {};"
    query_list = [rm_properties_query]
    query_tag = 'Removing properties of all the nodes'
    run_list_of_cypher_queries(bolt_url, user_name, password, query_list, query_tag)
    print('\n1) Removed properties of all the nodes.')
    rm_nodes_and_rels_query = "MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r"
    query_list = [rm_nodes_and_rels_query]
    query_tag = 'Removing all the nodes and the relations'
    run_list_of_cypher_queries(bolt_url, user_name, password, query_list, query_tag)
    print('2) Removed all the nodes and the relations')
    print('\nSuccessfully removed all the elements of Neo4j Graph Database.\n')


def remove_graph_db(bolt_url, user_name, password):
    # Checking the connectivity to the neo4j db using the credentials supplied through settings file
    print('\nChecking the connectivity to the neo4j db using the credentials supplied (in 3 attempts).')
    print('Bolt URL: %s'%bolt_url)
    print('User: %s'%user_name)
    print('Password: %s\n'%('*'*len(password)))
    pass_attempts = 3
    ctr = 0
    success_flag = 0
    while (success_flag == 0) & (ctr < pass_attempts):
        print('(Attempt # %d) ... Trying to connect ... '%(ctr+1))
        if check_connection_to_graph_db(bolt_url, user_name, password):
            success_flag = 1
            print('\nConnection to the Neo4j Database is successful.')
        else:
            print('Connection to the Neo4j Database is unsuccessful.')
        ctr += 1
    # If the password is incorrect for all the three attempts
    if success_flag == 0:
        print('\nMaximum number of attempts are over. Please check the password or the connectiviy and re-run the main code to ingest the data again.\n')
        sys.exit()
    print('\nTrying to remove all nodes, relations and their properties ...')
    remove_all(bolt_url, user_name, password)

