$SPARK_HOME/bin/spark-submit \
  --class com.datastax.cassandra.examples.UpdateAirportCode \
  --master local[5] \
  /Users/supriyas/airwatch/cassandra-examples/run/cassandra-examples-jar-with-dependencies.jar \
  127.0.0.1