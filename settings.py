# Organization : saal.ai
# Author : saal.ai

# Import all the necessary modules
import os

# All the necessary constants for the data import

# Input data path
input_data_path = os.path.join(os.path.split(os.path.abspath(__file__))[0], 'data')

# user_name
user_name = "neo4j"
# bolt_url = "bolt://localhost:17687"
bolt_url = "bolt://localhost:7687"

password = "graph"

# Sleep time in seconds (no need to modify this often)
sleep_time = 1.0
