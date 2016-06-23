package com.datastax.cassandra.examples;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Created by supriyas on 5/11/16.
 */
public class LoadData {
    public static final void main(String[] args) {
        if (args == null || args.length != 1) {
            System.err.println("Please provide the path of the data file");
            System.exit(-1);
        }
        File dataFile = new File(args[0]);
        if (!dataFile.exists() || !dataFile.isFile()) {
            System.err.println("Please provide a valid data file path");
            System.exit(-1);
        }
        Cluster cluster = Cluster.builder()
                .addContactPoints("127.0.0.1")
                .build();

        Session session = cluster.connect("test");
        System.out.println("Connected to Cassandra Database");

        PreparedStatement ps1 = session.prepare("insert into flights (ID, YEAR, DAY_OF_MONTH, FL_DATE, AIRLINE_ID, CARRIER, FL_NUM, ORIGIN_AIRPORT_ID, ORIGIN, ORIGIN_CITY_NAME, ORIGIN_STATE_ABR, "+
                "DEST, DEST_CITY_NAME, DEST_STATE_ABR, DEP_TIME, ARR_TIME, ACTUAL_ELAPSED_TIME, AIR_TIME, DISTANCE)"+
                " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        PreparedStatement ps2 = session.prepare("insert into flights_by_origin_airport (FL_DATE, AIRLINE_ID, CARRIER, FL_NUM, ORIGIN_AIRPORT_ID, DEP_TIME)"+
                " values (?, ?, ?, ?, ?, ?)");
        PreparedStatement ps3 = session.prepare("insert into flights_by_flight_num (CARRIER, FL_NUM, ORIGIN, DEST, AIR_TIME)"+
                " values (?, ?, ?, ?, ?)");

        String line = "";
        int successCntFlights = 0;
        int successCntFlightsByOriginAirport = 0;
        int successCntFlightsByFlightNum = 0;

        int totalCnt = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(dataFile))) {
            while ((line = br.readLine()) != null) {
                totalCnt++;
                System.out.println("Processing line: '" + line + "'");
                BoundStatement bound = null;
                FlightInfo flightInfo = new FlightInfo(line);

                try {
                    bound = ps1.bind()
                            .setInt(0, flightInfo.id)
                            .setInt(1, flightInfo.year)
                            .setInt(2, flightInfo.dayOfMonth)
                            .setTimestamp(3, flightInfo.flDate)
                            .setInt(4, flightInfo.airlineId)
                            .setString(5, flightInfo.carrier)
                            .setInt(6, flightInfo.flNum)
                            .setInt(7, flightInfo.originAirportId)
                            .setString(8, flightInfo.origin)
                            .setString(9, flightInfo.originCityName)
                            .setString(10, flightInfo.originStateAbr)
                            .setString(11, flightInfo.dest)
                            .setString(12, flightInfo.destCityName)
                            .setString(13, flightInfo.destStateAbr)
                            .setTimestamp(14, flightInfo.depTime)
                            .setTimestamp(15, flightInfo.arrTime)
                            .setTimestamp(16, flightInfo.actualElapsedTime)
                            .setTimestamp(17, flightInfo.airTime)
                            .setInt(18, flightInfo.distance);
                    session.execute(bound);
                    successCntFlights++;
                }
                catch (Exception ex) {
                    ex.printStackTrace();
                    System.err.println("Unable to load line in flights table: '"+line+"', error: "+ex.getMessage()+"!!!!!!!!!!!!!");
                }

                try {
                    bound = ps2.bind()
                            .setTimestamp(0, flightInfo.flDate)
                            .setInt(1, flightInfo.airlineId)
                            .setString(2, flightInfo.carrier)
                            .setInt(3, flightInfo.flNum)
                            .setInt(4, flightInfo.originAirportId)
                            .setTimestamp(5, flightInfo.depTime);
                    session.execute(bound);
                    successCntFlightsByOriginAirport++;
                }
                catch (Exception ex) {
                    ex.printStackTrace();
                    System.err.println("Unable to load line in flights_by_origin_airport table: '"+line+"', error: "+ex.getMessage()+"!!!!!!!!!!!!!");
                }

                try {
                    bound = ps3.bind()
                            .setString(0, flightInfo.carrier)
                            .setInt(1, flightInfo.flNum)
                            .setString(2, flightInfo.origin)
                            .setString(3, flightInfo.dest)
                            .setTimestamp(4, flightInfo.airTime);
                    session.execute(bound);
                    successCntFlightsByFlightNum++;
                }
                catch (Exception ex) {
                    ex.printStackTrace();
                    System.err.println("Unable to load line in flights_by_flight_num table: '"+line+"', error: "+ex.getMessage()+"!!!!!!!!!!!!!");
                }

            }
            System.out.println("Total records processed: "+totalCnt);
            System.out.println("Total records successfully inserted in flights table: "+successCntFlights);
            System.out.println("Total records successfully inserted in flights_by_origin_airport table: "+successCntFlightsByOriginAirport);
            System.out.println("Total records successfully inserted in flights_by_flight_num table: "+successCntFlightsByFlightNum);
        }
        catch (Throwable ex) {
            ex.printStackTrace();
            System.err.println("Unexpected error: "+ex.getMessage()+"!!!!!!");
        }
        finally {
            if (session != null) {
                session.close();
            }
        }
        System.exit(0);
    }

    private static class FlightInfo {
        private int id;
        private int year;
        private int dayOfMonth;
        private Date flDate;
        private int airlineId;
        private String carrier;
        private int flNum;
        private int originAirportId;
        private String origin;
        private String originCityName;
        private String originStateAbr;
        private String dest;
        private String destCityName;
        private String destStateAbr;
        private Date depTime;
        private Date arrTime;
        private Date actualElapsedTime;
        private Date airTime;
        private int distance;

        private static final String COMMA_SPLIT = ",";

        private static final SimpleDateFormat dateFormatter1 = new SimpleDateFormat("yyyy/MM/dd");
        private static final SimpleDateFormat dateFormatter2 = new SimpleDateFormat("HHmm");

        public FlightInfo(String line) throws Exception{
            if (line == null || line.isEmpty()) {
                throw new Exception("Invalid flight information passed!!!!!!!");
            }

            String[] data = line.split(COMMA_SPLIT);
            if (data.length != 19) {
                throw new Exception("Invalid flight information passed!!!!!!!");
            }

            id = Integer.parseInt(data[0]);
            year = Integer.parseInt(data[1]);
            dayOfMonth = Integer.parseInt(data[2]);
            flDate = dateFormatter1.parse(data[3]);
            airlineId = Integer.parseInt(data[4]);
            carrier = data[5];
            flNum = Integer.parseInt(data[6]);
            originAirportId = Integer.parseInt(data[7]);
            origin = data[8];
            originCityName = data[9];
            originStateAbr = data[10];
            dest = data[11];
            destCityName = data[12];
            destStateAbr = data[13];
            depTime = dateFormatter2.parse(formatIntoHourMin(data[14]));
            arrTime = dateFormatter2.parse(formatIntoHourMin(data[15]));
            actualElapsedTime = dateFormatter2.parse(formatIntoHourMin(data[16]));
            airTime = dateFormatter2.parse(formatIntoHourMin(data[17]));
            distance = Integer.parseInt(data[18]);
        }

        private String formatIntoHourMin(String time) throws Exception {
            if (time == null || time.trim().isEmpty() || time.trim().length() > 4) {
                throw new Exception("Invalid time format!!!!!");
            }

            String padding = "";

            time = time.trim();
            if (time.length() == 1) {
                padding = "000";
            }
            else
            if (time.length() == 2) {
                padding = "00";
            }
            else
            if (time.length() == 3) {
                padding = "0";
            }

            return padding+time;
        }
    }
}
