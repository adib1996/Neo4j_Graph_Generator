# Neo4J Graph DataBase Generator 

A light weight python application used to create a graph Database for Neo4j through an Excel file.

The following tools are needed:
1. Python (3.x version or above)
2. Neo4j (Neo4j 3.5.6 or below)

The application will use a sample dataset in the ```raw_input``` file which has been derived from [Georgetown University Public Dahlgren Memorial Library](https://guides.dml.georgetown.edu/infectiousdiseases/Databases)

The Very basic level of the data architecture of the Database could be represented like this:

![Diagram](./assets/basic_arch.png)

Before running the python scripts. Ensure you have Neo4j Desktop installed with a local server running at port ```bolt://localhost:7687```
The default password is set to `graph` in the `settings.py` file. You can change or disable the auth protocol in the graph DB

Run
>$ pip install -r requirements.txt

Then run the below command to formulate the nodes and relationships into a csv file
> python transform.py

Once the `data.csv` has been generated. Run
> python generate.py

Sanity check:
1. Total number of diseases: 25
2. Total nodes: 861
3. Total Relationships: 1195

