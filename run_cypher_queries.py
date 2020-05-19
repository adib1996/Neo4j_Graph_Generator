
# Importing necessary modules
import sys
import time
from neo4j.v1 import GraphDatabase, basic_auth
import ssl


def check_connection_to_graph_db(bolt_url, user_name, password):
    try:
        _ = GraphDatabase.driver(bolt_url, auth=basic_auth(user_name, password))
        return True
    except:
        return False


def get_merge_query_string(nodevar, nodelabel, tagList):
    tagList = [x for x in tagList if x[1] != ""]
    keyValueStringList = [x[0]+':"'+x[1]+'"' for x in tagList]
    keyValueString = ', '.join(keyValueStringList)
    merge_query_string = '''MERGE (''' + nodevar + ''':`''' + nodelabel + '''` {''' + keyValueString + '''})'''
    return merge_query_string


def get_match_query_string(nodevar, nodelabel, tagList):
    tagList = [x for x in tagList if x[1] != ""]
    keyValueStringList = [x[0]+':"'+x[1]+'"' for x in tagList]
    keyValueString = ', '.join(keyValueStringList)
    match_query_string = '''MATCH (''' + nodevar + ''':`''' + nodelabel + '''` {''' + keyValueString + '''})'''
    return match_query_string


def run_list_of_cypher_queries(bolt_url, user_name, password, query_list, query_tag):
    success_flag = 0
    number_iterations = 10
    ctr = 0
    while (ctr < number_iterations) & (success_flag == 0):
        if ctr > 0:
            print('Retrying ...')
        try:
            driver = GraphDatabase.driver(bolt_url, auth=basic_auth(user_name, password))
            # if (ctr == 0):
            # print('Connected to Graph DB @ %s.'%(query_tag))
            # else:
            # print('Connected to Graph DB @ %s in Attempt Number %d.'%(query_tag, ctr+1))
            session = driver.session()
            for q in query_list:
                # print("\n\nQuery: \n%s"%q)
                session.run(q)
            session.close()
            success_flag = 1
        except ssl.SSLError as e:
            # print('Failed to connect to Graph DB @ %s in Attempt Number %d.'%(query_tag, ctr+1))
            print('Error: '+str(e))
            time.sleep(1.0)
            success_flag = 0
        ctr += 1
    if (ctr >= number_iterations) & (success_flag == 0):
        print('\nNot able to connect to Neo4j Database even after 10 attempts. Please check the credentials.\n')
        sys.exit()


def replace_double_quote_with_single_quote(x):
    return str(x).replace('"',"'")


def create_main_node(bolt_url, user_name, password, node_label, node_base_attribute, node_name):
    node_name = replace_double_quote_with_single_quote(node_name)
    tagList = [[node_base_attribute, node_name]]
    query = get_merge_query_string("x", node_label, tagList)
    query_list = [query]
    query_tag = 'Created main node: %s'%node_name
    run_list_of_cypher_queries(bolt_url, user_name, password, query_list, query_tag)


def add_attribute_to_main_node(bolt_url, user_name, password, start_node_label, \
                               start_node_base_attribute, start_node_name, attribute_name, attribute_value):
    start_node_name = replace_double_quote_with_single_quote(start_node_name)
    attribute_value = replace_double_quote_with_single_quote(attribute_value)
    tagList = [[start_node_base_attribute, start_node_name]]
    query = get_match_query_string("x", start_node_label, tagList) + ''' SET x.''' + attribute_name + ''' = "''' + attribute_value + '''"'''
    query_list = [query]
    query_tag = 'Added %s attribute to %s node'%(attribute_name, start_node_name)
    run_list_of_cypher_queries(bolt_url, user_name, password, query_list, query_tag)


def create_relation_between_nodes(bolt_url, user_name, password, \
                                  start_node_label, start_node_base_attribute, start_node_name, \
                                  relation_name, relation_attribute_name, relation_attribute_value, \
                                  end_node_label, end_node_base_attribute, end_node_name):
    start_node_name = replace_double_quote_with_single_quote(start_node_name)
    end_node_name = replace_double_quote_with_single_quote(end_node_name)
    # Start Node match query
    startNodeTagList = [[start_node_base_attribute, start_node_name]]
    start_node_match_query = get_match_query_string("x", start_node_label, startNodeTagList)
    # End Node match query
    endNodeTagList = [[end_node_base_attribute, end_node_name]]
    end_node_match_query = get_match_query_string("y", end_node_label, endNodeTagList)
    # Relation query
    rel_query = '''MERGE (x) -[:`''' + relation_name + '''`]-> (y)'''
    if (relation_attribute_name != "") & (relation_attribute_value != ""):
        rel_query = '''MERGE (x) -[:`''' + relation_name + '''` {''' + relation_attribute_name + ''' :"''' + \
                    relation_attribute_value + '''"}]-> (y)'''
    # Full query
    query = ' '.join((start_node_match_query, 'WITH x', end_node_match_query, 'WITH x,y', rel_query))
    # print('\n\nQuery: \n%s\n' % query)
    query_list = [query]
    query_tag = 'Added %s relation between nodes (%s -> %s)' % (relation_name, start_node_name, end_node_name)
    run_list_of_cypher_queries(bolt_url, user_name, password, query_list, query_tag)