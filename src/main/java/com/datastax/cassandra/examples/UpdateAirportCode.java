package com.datastax.cassandra.examples;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Date;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;


/**
 * Created by supriyas on 5/13/16.
 */
public class UpdateAirportCode {
    public static final void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Please provide the cassandra host information!!!!!!");
        }

        SparkConf sparkConf = new SparkConf().setAppName("Update Airport Code");
        sparkConf.set("spark.cassandra.connection.host", args[0]);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        Accumulator<Integer> accum1 = sc.accumulator(0);
        Accumulator<Integer> accum2 = sc.accumulator(0);

        JavaRDD<FlightInfo> rdd = javaFunctions(sc).cassandraTable("test", "flights", mapRowTo(FlightInfo.class))
                .filter(flightInfo1 -> flightInfo1.origin.equalsIgnoreCase("BOS"))
                .map(flightInfo -> {
                    accum1.add(1);
                    flightInfo.origin = "TOS";
                    return flightInfo;
                });
        javaFunctions(rdd)
                .writerBuilder("test", "flights", mapToRow(FlightInfo.class))
                .saveToCassandra();

        rdd = javaFunctions(sc).cassandraTable("test", "flights", mapRowTo(FlightInfo.class))
                .filter(flightInfo1 -> flightInfo1.dest.equalsIgnoreCase("BOS"))
                .map(flightInfo -> {
                    accum1.add(1);
                    flightInfo.dest = "TOS";
                    return flightInfo;
                });
        javaFunctions(rdd)
                .writerBuilder("test", "flights", mapToRow(FlightInfo.class))
                .saveToCassandra();
        System.out.println("Total Number of records updated in flights table:"+accum1.value());

        JavaRDD<FlightsByFlightNum> rdd1 = javaFunctions(sc).cassandraTable("test", "flights_by_flight_num", mapRowTo(FlightsByFlightNum.class))
                .filter(flightsByFlightNum1 -> flightsByFlightNum1.origin.equalsIgnoreCase("BOS"))
                .map(flightsByFlightNum -> {
                    accum2.add(1);
                    flightsByFlightNum.origin = "TOS";
                    return flightsByFlightNum;
                });
        javaFunctions(rdd1)
                .writerBuilder("test", "flights_by_flight_num", mapToRow(FlightsByFlightNum.class))
                .saveToCassandra();

        rdd1 = javaFunctions(sc).cassandraTable("test", "flights_by_flight_num", mapRowTo(FlightsByFlightNum.class))
                .filter(flightsByFlightNum1 -> flightsByFlightNum1.dest.equalsIgnoreCase("BOS"))
                .map(flightsByFlightNum -> {
                    accum2.add(1);
                    flightsByFlightNum.dest = "TOS";
                    return flightsByFlightNum;
                });
        javaFunctions(rdd1)
                .writerBuilder("test", "flights_by_flight_num", mapToRow(FlightsByFlightNum.class))
                .saveToCassandra();
        System.out.println("Total Number of records updated in flights_by_flight_num table:" + accum2.value());


        sc.stop();
    }

    public static class FlightInfo {
        private int id;
        private int year;
        private int day_of_month;
        private Date fl_date;
        private int airline_id;
        private String carrier;
        private int fl_num;
        private int origin_airport_id;
        private String origin;
        private String origin_city_name;
        private String origin_state_abr;
        private String dest;
        private String dest_city_name;
        private String dest_state_abr;
        private Date dep_time;
        private Date arr_time;
        private Date actual_elapsed_time;
        private Date air_time;
        private int distance;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getYear() {
            return year;
        }

        public void setYear(int year) {
            this.year = year;
        }

        public int getDay_of_month() {
            return day_of_month;
        }

        public void setDay_of_month(int day_of_month) {
            this.day_of_month = day_of_month;
        }

        public Date getFl_date() {
            return fl_date;
        }

        public void setFl_date(Date fl_date) {
            this.fl_date = fl_date;
        }

        public int getAirline_id() {
            return airline_id;
        }

        public void setAirline_id(int airline_id) {
            this.airline_id = airline_id;
        }

        public String getCarrier() {
            return carrier;
        }

        public void setCarrier(String carrier) {
            this.carrier = carrier;
        }

        public int getFl_num() {
            return fl_num;
        }

        public void setFl_num(int fl_num) {
            this.fl_num = fl_num;
        }

        public int getOrigin_airport_id() {
            return origin_airport_id;
        }

        public void setOrigin_airport_id(int origin_airport_id) {
            this.origin_airport_id = origin_airport_id;
        }

        public String getOrigin() {
            return origin;
        }

        public void setOrigin(String origin) {
            this.origin = origin;
        }

        public String getOrigin_city_name() {
            return origin_city_name;
        }

        public void setOrigin_city_name(String origin_city_name) {
            this.origin_city_name = origin_city_name;
        }

        public String getOrigin_state_abr() {
            return origin_state_abr;
        }

        public void setOrigin_state_abr(String origin_state_abr) {
            this.origin_state_abr = origin_state_abr;
        }

        public String getDest() {
            return dest;
        }

        public void setDest(String dest) {
            this.dest = dest;
        }

        public String getDest_city_name() {
            return dest_city_name;
        }

        public void setDest_city_name(String dest_city_name) {
            this.dest_city_name = dest_city_name;
        }

        public String getDest_state_abr() {
            return dest_state_abr;
        }

        public void setDest_state_abr(String dest_state_abr) {
            this.dest_state_abr = dest_state_abr;
        }

        public Date getDep_time() {
            return dep_time;
        }

        public void setDep_time(Date dep_time) {
            this.dep_time = dep_time;
        }

        public Date getArr_time() {
            return arr_time;
        }

        public void setArr_time(Date arr_time) {
            this.arr_time = arr_time;
        }

        public Date getActual_elapsed_time() {
            return actual_elapsed_time;
        }

        public void setActual_elapsed_time(Date actual_elapsed_time) {
            this.actual_elapsed_time = actual_elapsed_time;
        }

        public Date getAir_time() {
            return air_time;
        }

        public void setAir_time(Date air_time) {
            this.air_time = air_time;
        }

        public int getDistance() {
            return distance;
        }

        public void setDistance(int distance) {
            this.distance = distance;
        }
    }

    public static class FlightsByFlightNum {
        private String carrier;
        private int fl_num;
        private String origin;
        private String dest;
        private Date air_time;

        public String getCarrier() {
            return carrier;
        }

        public int getFl_num() {
            return fl_num;
        }

        public String getOrigin() {
            return origin;
        }

        public String getDest() {
            return dest;
        }

        public Date getAir_time() {
            return air_time;
        }

        public void setCarrier(String carrier) {
            this.carrier = carrier;
        }

        public void setFl_num(int fl_num) {
            this.fl_num = fl_num;
        }

        public void setOrigin(String origin) {
            this.origin = origin;
        }

        public void setDest(String dest) {
            this.dest = dest;
        }

        public void setAir_time(Date air_time) {
            this.air_time = air_time;
        }
    }
}
