1. The cqls in the file cassandra_DDL_Statements.txt are used to create the key space 'test', the main data column family 'flights'
and the two query column families 'flights_by_origin_airport' and 'flights_by_flight_num' for the two query requirements.

2. The java class com.datastax.cassandra.examples.LoadData, loads the data into the cassandra tables from the supplied data file flights_from_pg.csv.
   Running the class LoadData takes only one argument, the full path to the data file.

Note: No error during data load. All data from the load csv file successfully loads to the cassandra tables.

3. The file query_cqls.txt provides the two queries, one to retrieve data for a specific origin airport sorted by departure time, the other to retrieve data for a specific flight
   in 10 min block of air time. The result of first query is in file query1_output.txt and the result of the second query is in file
   query2_output.txt.

4. Please check the file ‘Information on query execution.txt’ to see how sold index is created on keycap test and column family ‘flights’.

5. The java class com.datastax.cassandra.examples.UpdateAirportCode, updates the airport code 'BOS' to 'TOS' in the tables 'flights' and
   flights_by_flight_num for both origin and departure airports by submitting Spark jobs. The script to run the spark job is under folder
   'run' together with the all included jar file cassandra-examples-jar-with-dependencies.jar.


