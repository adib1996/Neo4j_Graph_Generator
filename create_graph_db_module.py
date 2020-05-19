
# Importing the necessary modules
import os
import sys
from settings import *
import pandas as pd
from read_data_module import read_data
from custom_utils import custom_status_bar
from run_cypher_queries import *
import getpass
import time


def create_graph_db(input_data_path, bolt_url, user_name, password):
    # Reading all the csv files in the data folder and creating the final dataset
    triplets_file_names = os.listdir(input_data_path)
    triplets_file_names = [x for x in triplets_file_names if ('.csv' in x)]

    # Checking for the count of files
    if len(triplets_file_names) == 0:
        print('\nThere are no csv files in the input directory.\n')
        sys.exit()

    print('\nList of input data files:')
    print(triplets_file_names)

    # Reading data from the input triples files
    data = read_data(input_data_path, triplets_file_names)

    # Showing the top rows of the data
    # print('\nTop rows in the input Dataset:')
    # print(data.head())

    # Checking the connectivity to the neo4j db using the credentials supplied through settings file
    print('\nChecking the connectivity to the neo4j db using the credentials supplied (in 3 attempts).')
    print('Bolt URL: %s' % bolt_url)
    print('User: %s' % user_name)
    print('Password: %s\n' % ('*' * len(password)))
    pass_attempts = 3
    ctr = 0
    success_flag = 0
    while (success_flag == 0) & (ctr < pass_attempts):
        print('(Attempt # %d) ... Trying to connect ... ' % (ctr + 1))
        if check_connection_to_graph_db(bolt_url, user_name, password):
            success_flag = 1
            print('\nConnection to the Neo4j Database is successful.')
        else:
            print('Connection to the Neo4j Database is unsuccessful.')
        ctr += 1

    # If the password is incorrect for all the three attempts
    if success_flag == 0:
        print('\nMaximum number of attempts are over. Please check the password or the \
              connectiviy and re-run the main code to ingest the data again.\n')
        sys.exit()

    # Creating the main nodes (to avoid repetition)
    recordTags = list(data["Record Tag"].unique())
    t1 = time.time()
    print("\nCreating all the nodes and adding attributes, relations, additional labels and more ...")
    for j in range(len(recordTags)):
        subset = data.loc[data["Record Tag"] == recordTags[j]].reset_index(drop=True)
        unique_start_nodes = subset[["Start Node Label", "Start Node Attribute Name", "Start Node Attribute Value"]]. \
                                drop_duplicates().reset_index(drop=True)
        unique_start_nodes.columns = ["Node Label", "Node Attribute Name", "Node Attribute Value"]
        unique_end_nodes = subset[["End Node Label", "End Node Attribute Name", "End Node Attribute Value"]]. \
                                drop_duplicates().reset_index(drop=True)
        unique_end_nodes.columns = ["Node Label", "Node Attribute Name", "Node Attribute Value"]
        unique_nodes = pd.concat([unique_start_nodes, unique_end_nodes]).reset_index(drop=True)
        unique_nodes = unique_nodes.loc[((unique_nodes["Node Attribute Name"] != "") &
                                         (unique_nodes["Node Label"] != "")), :]. \
                                drop_duplicates().reset_index(drop=True)

        # Looping through and creating the main nodes
        for a in range(unique_nodes.shape[0]):
            node_label = unique_nodes["Node Label"].iloc[a].strip()
            node_attribute_name = unique_nodes["Node Attribute Name"].iloc[a].strip()
            node_attribute_value = unique_nodes["Node Attribute Value"].iloc[a].strip()
            create_main_node(bolt_url, user_name, password, node_label, node_attribute_name, node_attribute_value)
        for i in range(subset.shape[0]):            
            start_node_label = subset["Start Node Label"].iloc[i].strip()
            start_node_attribute_name = subset["Start Node Attribute Name"].iloc[i].strip()
            start_node_attribute_value = subset["Start Node Attribute Value"].iloc[i].strip()
            graph_property = subset["Graph Element"].iloc[i].strip()
            if graph_property.lower() == "attribute":
                attribute_name = subset["Attribute Name"].iloc[i].strip()
                attribute_value = subset["Attribute Value"].iloc[i].strip()
                if attribute_value != "":
                    add_attribute_to_main_node(bolt_url, user_name, password,
                                               start_node_label, start_node_attribute_name,
                                               start_node_attribute_value,
                                               attribute_name, attribute_value)
            if graph_property.lower() == "relation":
                relation_name = subset["Relation Name"].iloc[i].strip()
                relation_attribute_name = subset["Relation Attribute Name"].iloc[i].strip()
                relation_attribute_value = subset["Relation Attribute Value"].iloc[i].strip()
                end_node_label = subset["End Node Label"].iloc[i].strip()
                end_node_attribute_name = subset["End Node Attribute Name"].iloc[i].strip()
                end_node_attribute_value = subset["End Node Attribute Value"].iloc[i].strip()
                create_relation_between_nodes(bolt_url, user_name, password,
                                              start_node_label, start_node_attribute_name, start_node_attribute_value, 
                                              relation_name, relation_attribute_name, relation_attribute_value,
                                              end_node_label, end_node_attribute_name, end_node_attribute_value)
        # Printing the status bar
        custom_status_bar(j + 1, len(recordTags), t1, time.time())

    print('\n\nCompleted importing the csv data into Neo4j Database.')
