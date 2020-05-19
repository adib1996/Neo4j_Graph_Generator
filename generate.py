# Organization : saal.ai
# Author : saal.ai

# Import the necessary modules
from settings import *
from custom_utils import get_time_string
from create_graph_db_module import create_graph_db
from remove_graph_db_module import remove_graph_db
from check_internet_connection import show_internet_connection
import time

# Checking for the internet connection
show_internet_connection()

# Start time
t0 = time.time()

# Removing the existing graph db
rem_db_str = "Step # 1 : Removing the Graph DB"
print("\n%s"%("="*len(rem_db_str)))
print(rem_db_str)
print("%s\n"%("="*len(rem_db_str)))
remove_graph_db(bolt_url, user_name, password)

# Creating the new graph db
create_db_str = "Step # 2 : Creating the Graph DB"
print("\n%s"%("="*len(create_db_str)))
print(create_db_str)
print("%s\n"%("="*len(create_db_str)))
create_graph_db(input_data_path, bolt_url, user_name, password)

print ('\nTotal time taken: %s.\n'%(get_time_string(time.time()-t0)))
