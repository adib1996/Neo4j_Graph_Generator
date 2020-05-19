# Import the necessary modules
from settings import *
from custom_utils import get_time_string
from remove_graph_db_module import remove_graph_db
from check_internet_connection import show_internet_connection
import time

# Checking for the internet connection
show_internet_connection()

# Start time
t0 = time.time()

# Removing the existing graph db
rem_db_str = "Removing the Graph DB ..."
print("\n%s"%("="*len(rem_db_str)))
print(rem_db_str)
print("%s\n"%("="*len(rem_db_str)))
remove_graph_db(bolt_url, user_name, password)

print('\nTotal time taken: %s.\n'%(get_time_string(time.time()-t0)))
