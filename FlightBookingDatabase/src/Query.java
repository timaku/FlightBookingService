import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.FileInputStream;
import java.sql.Types;

/**
 * Runs queries against a back-end database
 */
public class Query {
	
	class Itinerary {
		private int fid1;
		private int fid2;
		private int day;
		
		public Itinerary (int fid1, int fid2, int day) {
			this.fid1=fid1;
			this.fid2 = fid2;
			this.day = day;
		}
		
		public String toString() {
			return "[" + fid1 + ", " + fid2 + ", " + day + "]";
		}
	}

	private String configFilename;
	private Properties configProps = new Properties();

	private String jSQLDriver;
	private String jSQLUrl;
	private String jSQLUser;
	private String jSQLPassword;

	// DB Connection
	private Connection conn;

	// Logged In User
	private String username;
	boolean loggedIn;
	

	// Current itinerary index
	private int itIndex;
	
	// Store recent search results of the user
	private Map<Integer, Itinerary> history;
	
	// Canned queries

       // search (one hop) -- This query ignores the month and year entirely. You can change it to fix the month and year
       // to July 2015 or you can add month and year as extra, optional, arguments
	private static final String SEARCH_ONE_HOP_SQL =
			"SELECT TOP (?) year,month_id,day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,fid "
					+ "FROM FLIGHTS, MONTHS "
					+ "WHERE origin_city = ? AND dest_city = ? AND day_of_month = ? AND year = 2015 AND MONTHS.month = 'July' AND MONTHS.mid = FLIGHTS.month_id AND actual_time IS NOT NULL "
					+ "ORDER BY actual_time ASC";
	private PreparedStatement searchOneHopStatement;

       // TODO: Add more queries here
	private static final String DELETE_RESERVATION = "DELETE FROM RESERVATIONS WHERE rid = ? AND username = ?";
	private PreparedStatement deleteReservationStatement;
	
	private static final String CHECK_RESERVATION = "SELECT * FROM RESERVATIONS WHERE rid = ? AND username = ?";
	private PreparedStatement checkReservationStatement;
	
	private static final String PULL_FLIGHT_INFO = "SELECT * FROM FLIGHTS WHERE fid = ?";
	private PreparedStatement pullFlightInfoStatement;
	
	
	private static final String PULL_RESERVATIONS = "SELECT * FROM RESERVATIONS WHERE username = ?";
	private PreparedStatement pullReservationsStatement;
	
	private static final String CHECK_DAYS = "SELECT day FROM RESERVATIONS WHERE username = ?";
	private PreparedStatement checkDaysStatement;
	
	private static final String UPDATE_CAPACITY = "UPDATE CAPACITY SET taken_seats = ? WHERE fid = ?";
	private PreparedStatement updateCapacityStatement;
	
	private static final String PULL_MAX_RID = "SELECT max(rid) FROM RESERVATIONS WHERE username = ?";
	private PreparedStatement pullMaxRIDStatement;
	
	private static final String CHECK_FLIGHT_SPACE = "SELECT * FROM CAPACITY WHERE fid = ?";
	private PreparedStatement checkFlightSpaceStatement;
	
	private static final String INSERT_TO_RESERVATIONS = "INSERT INTO RESERVATIONS VALUES(?, ?, ?, ?, ?)";
	private PreparedStatement insertReservationStatement;
	
	private static final String SEARCH_MULTI_HOP_SQL =
			"SELECT TOP (?) a.fid AS afid,a.carrier_id AS acid,a.flight_num AS afnum,a.origin_city AS aocity,a.dest_city AS adcity,a.actual_time AS atime,"
			+ "b.fid AS bfid,b.carrier_id AS bcid,b.flight_num AS bfnum,b.origin_city AS bocity,b.dest_city AS bdcity,b.actual_time AS btime,a.actual_time + b.actual_time AS total_time "
					+ "FROM FLIGHTS a,FLIGHTS b, MONTHS "
					+ "WHERE a.origin_city = ? AND a.dest_city = b.origin_city AND b.dest_city = ? AND a.year = 2015 AND MONTHS.month = 'July' AND MONTHS.mid = a.month_id "
					+ "AND a.year = b.year AND a.month_id = b.month_id AND a.day_of_month = b.day_of_month AND a.day_of_month = ? AND a.actual_time IS NOT NULL AND b.actual_time IS NOT NULL "
					+ "ORDER BY total_time ASC";
	private PreparedStatement searchMultiHopStatement;
	
	private static final String SEARCH_LOGIN_PASSWORD =
			"SELECT * "
			+ "FROM CUSTOMERS "
			+ "WHERE username = ? AND password = ?";
	private PreparedStatement searchLoginPasswordStatement;
	
	// transactions
	private static final String BEGIN_TRANSACTION_SQL =  
			"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;"; 
	private PreparedStatement beginTransactionStatement;

	private static final String COMMIT_SQL = "COMMIT TRANSACTION";
	private PreparedStatement commitTransactionStatement;

	private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
	private PreparedStatement rollbackTransactionStatement;


	public Query(String configFilename) {
		this.configFilename = configFilename;
		itIndex = 1;
		history = new HashMap<Integer, Itinerary>();
	}

	/**********************************************************/
	/* Connection code to SQL Azure.  */
	public void openConnection() throws Exception {
		configProps.load(new FileInputStream(configFilename));

		jSQLDriver   = configProps.getProperty("flightservice.jdbc_driver");
		jSQLUrl	   = configProps.getProperty("flightservice.url");
		jSQLUser	   = configProps.getProperty("flightservice.sqlazure_username");
		jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

		/* load jdbc drivers */
		Class.forName(jSQLDriver).newInstance();

		/* open connections to the flights database */
		conn = DriverManager.getConnection(jSQLUrl, // database
				jSQLUser, // user
				jSQLPassword); // password

		conn.setAutoCommit(true); //by default automatically commit after each statement 

		/* You will also want to appropriately set the 
                   transaction's isolation level through:  
		   conn.setTransactionIsolation(...) */

	}

	public void closeConnection() throws Exception {
		conn.close();
	}

	/**********************************************************/
	/* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

	public void prepareStatements() throws Exception {
		searchOneHopStatement = conn.prepareStatement(SEARCH_ONE_HOP_SQL);
 		beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
		commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
		rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);

		/* add here more prepare statements for all the other queries you need */
		/* . . . . . . */
		searchLoginPasswordStatement = conn.prepareStatement(SEARCH_LOGIN_PASSWORD);
		searchMultiHopStatement = conn.prepareStatement(SEARCH_MULTI_HOP_SQL);
		checkFlightSpaceStatement = conn.prepareStatement(CHECK_FLIGHT_SPACE);
		insertReservationStatement = conn.prepareStatement(INSERT_TO_RESERVATIONS);
		pullMaxRIDStatement = conn.prepareStatement(PULL_MAX_RID);
		updateCapacityStatement = conn.prepareStatement(UPDATE_CAPACITY);
		checkDaysStatement = conn.prepareStatement(CHECK_DAYS);
		pullFlightInfoStatement = conn.prepareStatement(PULL_FLIGHT_INFO);
		pullReservationsStatement = conn.prepareStatement(PULL_RESERVATIONS);
		checkReservationStatement = conn.prepareStatement(CHECK_RESERVATION);
		deleteReservationStatement = conn.prepareStatement(DELETE_RESERVATION);
	}
	
	public void transaction_login(String username, String password) throws Exception {
		
		int attempts = 1;
		outer: while(true) {
			if(attempts > 20) {
				System.out.println("Transaction has failed: " + attempts + " times, aborting...");
			}
			try {
				beginTransaction();
			
				searchLoginPasswordStatement.clearParameters();
				searchLoginPasswordStatement.setString(1, username);
				searchLoginPasswordStatement.setString(2, password);
		
				ResultSet loginResults = searchLoginPasswordStatement.executeQuery();
				String uname = "";
		        		
				if (loginResults.next()) {
			        uname = loginResults.getString("username");
			        this.username = uname;
			        loggedIn = true;
			        //get ready to record search history!
			        itIndex = 1;
			        history.clear();			
			        
	
			        System.out.println("Login successful, " + uname + "!");
		  		} else {
					System.out.println("Login failed, please try again!");
				}			        
				commitTransaction();
				loginResults.close();
				break outer;
			} catch (SQLException e) {
				rollbackTransaction();
				attempts++;
			}
		}
	}

	
	/**
	 * Searches for flights from the given origin city to the given destination
	 * city, on the given day of the month. If "directFlight" is true, it only
	 * searches for direct flights, otherwise is searches for direct flights
	 * and flights with two "hops". Only searches for up to the number of
	 * itineraries given.
	 * Prints the results found by the search.
	 */
	public void transaction_search_safe(String originCity, String destinationCity, boolean directFlight, int dayOfMonth, int numberOfItineraries) throws Exception {
		
		boolean hasResults = false;
		//clear search history
		history.clear();
		itIndex = 1;
		int offset = 1;
		
		/* keep searching until it we can grab the lock */
		int attempts1 = 1;
		outer: while(true) {
			if(attempts1 > 20) {
				System.out.println("Transaction has failed: " + attempts1 + " times, aborting...");
			}
			try {
				beginTransaction();
				
				// always print one-hop itineraries
				searchOneHopStatement.clearParameters();
				searchOneHopStatement.setInt(1, numberOfItineraries);
				searchOneHopStatement.setString(2, originCity);
				searchOneHopStatement.setString(3, destinationCity);
				searchOneHopStatement.setInt(4, dayOfMonth);
				ResultSet oneHopResults = searchOneHopStatement.executeQuery();
				while (oneHopResults.next()) {
					hasResults = true;
					int result_fid = oneHopResults.getInt("fid");
		            String result_carrierId = oneHopResults.getString("carrier_id");
		            int result_flightNum = oneHopResults.getInt("flight_num");
		            String result_originCity = oneHopResults.getString("origin_city");
		            String result_destCity = oneHopResults.getString("dest_city");
		            int result_time = oneHopResults.getInt("actual_time");
		            System.out.println("Itinerary " + itIndex + " Total Time: " + result_time + " minutes");
		            printFlights(result_carrierId, result_flightNum, result_originCity, result_destCity, result_time);
		            System.out.println();
		            
		            
		            Itinerary i = new Itinerary(result_fid, -1, dayOfMonth);
		            history.put(itIndex, i);
	
		            
		            itIndex++;
		            offset++;
		  		}
				
				commitTransaction();
				oneHopResults.close();
				break outer;
			} catch (SQLException e) {
				rollbackTransaction();
				attempts1++;
			}
		}
		int attempts = 1;
		outer: while(true) {
			if(attempts > 20) {
				System.out.println("Transaction has failed: " + attempts + " times, aborting...");
			}
			try {
				beginTransaction();
				//print 2 hop flights
				if(!directFlight) {
					searchMultiHopStatement.clearParameters();
					searchMultiHopStatement.setInt(1, numberOfItineraries - offset + 1);
					searchMultiHopStatement.setString(2, originCity);
					searchMultiHopStatement.setString(3, destinationCity);
					searchMultiHopStatement.setInt(4, dayOfMonth);
					ResultSet multiHopResults = searchMultiHopStatement.executeQuery();
					
					while (multiHopResults.next() && offset <= numberOfItineraries) {
						hasResults = true;
						
						int a_fid = multiHopResults.getInt("afid");
			            int b_fid = multiHopResults.getInt("bfid");
			            
			            String a_carrierId = multiHopResults.getString("acid");
			            String b_carrierId = multiHopResults.getString("bcid");
			            
			            int a_flightNum = multiHopResults.getInt("afnum");
			            int b_flightNum = multiHopResults.getInt("bfnum");
		
			            String a_originCity = multiHopResults.getString("aocity");
			            String b_originCity = multiHopResults.getString("bocity");
		
			            String a_destCity = multiHopResults.getString("adcity");
			            String b_destCity = multiHopResults.getString("bdcity");
		
			            int a_time = multiHopResults.getInt("atime");
			            int b_time = multiHopResults.getInt("btime");
			            int total_time = multiHopResults.getInt("total_time");
			            
			            System.out.println("Itinerary " + itIndex + " Total Time: " + total_time + " minutes");
			            printFlights(a_carrierId, a_flightNum, a_originCity, a_destCity, a_time);
			            printFlights(b_carrierId, b_flightNum, b_originCity, b_destCity, b_time);
			            System.out.println();
			            
			            
			            Itinerary i = new Itinerary(a_fid, b_fid, dayOfMonth);
			            history.put(itIndex, i);
			            
			            offset++;
			            itIndex++;
					}
					multiHopResults.close();
				}
				commitTransaction();
				break outer;
			} catch (SQLException e) {
				rollbackTransaction();
				attempts++;
			}
		}
	 
		if(!hasResults) {
			System.out.println("Sorry no itineraries matched your search. Please try again!");
		}
	}
	
	public void transaction_search_unsafe(String originCity, String destinationCity, boolean directFlight, int dayOfMonth, int numberOfItineraries) throws Exception {

            // one hop itineraries
            String unsafeSearchSQL =
                "SELECT TOP (" + numberOfItineraries +  ") year,month_id,day_of_month,carrier_id,flight_num,origin_city,actual_time "
                + "FROM Flights "
                + "WHERE origin_city = \'" + originCity + "\' AND dest_city = \'" + destinationCity +  "\' AND day_of_month =  " + dayOfMonth + " "
                + "ORDER BY actual_time ASC";

            System.out.println("Submitting query: " + unsafeSearchSQL);
            Statement searchStatement = conn.createStatement();
            ResultSet oneHopResults = searchStatement.executeQuery(unsafeSearchSQL);

            while (oneHopResults.next()) {
                int result_year = oneHopResults.getInt("year");
                int result_monthId = oneHopResults.getInt("month_id");
                int result_dayOfMonth = oneHopResults.getInt("day_of_month");
                String result_carrierId = oneHopResults.getString("carrier_id");
                String result_flightNum = oneHopResults.getString("flight_num");
                String result_originCity = oneHopResults.getString("origin_city");
                int result_time = oneHopResults.getInt("actual_time");
                System.out.println("Flight: " + result_year + "," + result_monthId + "," + result_dayOfMonth + "," + result_carrierId + "," + result_flightNum + "," + result_originCity + "," + result_time);
            }
            oneHopResults.close();
        }

	public void transaction_book(int itineraryId) throws Exception {
		
		if (!loggedIn) {
			System.out.println("Sorry you need to login to book!");
			return;
		}
		
		if (itineraryId >= itIndex || itineraryId < 1) {
			System.out.println("Not a valid itinerary number.");
			return;
		}
		
		//pull itinerary from history cache		
		int fid1;
		int fid2;
		int itiDay;
		if(history.containsKey(itineraryId)){
			Itinerary temp = history.get(itineraryId);
			fid1 = temp.fid1;
			fid2 = temp.fid2;
			itiDay = temp.day;
		} else {
			System.out.println("Sorry this itinerary doesn't exist");
			return;
		}
		
		int attempts = 1;
		outer:
		while(true) {
			if(attempts > 20) {
				System.out.println("Transaction has failed: " + attempts + " times, aborting...");
			}
			try {
				beginTransaction();
				//check for space on flights
				checkFlightSpaceStatement.clearParameters();
				checkFlightSpaceStatement.setInt(1, fid1);
				ResultSet checkFlightSpaceResults = checkFlightSpaceStatement.executeQuery();
				int max_seats;
				int taken_seats = 0;
				if(checkFlightSpaceResults.next()) {
					max_seats = checkFlightSpaceResults.getInt("max_seats");
					taken_seats = checkFlightSpaceResults.getInt("taken_seats");
					
					if(taken_seats >= max_seats) {
						System.out.println("Sorry! Flight " + fid1 + " is full! Try another itinerary");
						return;
					}
				}
				checkFlightSpaceResults.close();
				
				int max_seats2;
				int taken_seats2 = 0;
		
				if(fid2 != -1) {
					checkFlightSpaceStatement.clearParameters();
					checkFlightSpaceStatement.setInt(1, fid2);
					ResultSet checkFlightSpaceResults2 = checkFlightSpaceStatement.executeQuery();
					if(checkFlightSpaceResults2.next()) {
						max_seats2 = checkFlightSpaceResults2.getInt("max_seats");
						taken_seats2 = checkFlightSpaceResults2.getInt("taken_seats");
						if(taken_seats2 >= max_seats2) {
							System.out.println("Sorry! Flight " + fid2 + " is full! Try another itinerary");
							return;
						}
					}
					checkFlightSpaceResults2.close();
				}
				
				//now insert entry in reservations and increment taken seats
				
				
				//check days in reservation
				checkDaysStatement.clearParameters();
				checkDaysStatement.setString(1, this.username);
				ResultSet checkDaysResults = checkDaysStatement.executeQuery();
				while (checkDaysResults.next()) {
					int day = checkDaysResults.getInt("day");
					if (itiDay == day) {
						System.out.println("Sorry you have a booking on this day already. Try another itinerary.");
						return;
					}
				}
				checkDaysResults.close();
				
				//find max RID for this person
				pullMaxRIDStatement.clearParameters();
				pullMaxRIDStatement.setString(1, this.username);
				ResultSet pullMaxRIDResults = pullMaxRIDStatement.executeQuery();
				int maxRID;
				if(pullMaxRIDResults.next()) {
					maxRID = pullMaxRIDResults.getInt(1);
				} else {
					maxRID = 0;
				}
				pullMaxRIDResults.close();
				
				//actually insert
				insertReservationStatement.clearParameters();
				insertReservationStatement.setString(1, this.username);
				insertReservationStatement.setInt(2, maxRID + 1);
				insertReservationStatement.setInt(3, fid1);
				if (fid2 == -1) {
					insertReservationStatement.setNull(4, Types.INTEGER);
				} else {
					insertReservationStatement.setInt(4, fid2);
				}
		
				insertReservationStatement.setInt(5, itiDay);
				insertReservationStatement.execute();
				
				//update capacity
				updateCapacityStatement.clearParameters();
				updateCapacityStatement.setInt(2, fid1);
				updateCapacityStatement.setInt(1, taken_seats + 1);
				updateCapacityStatement.execute();
				
				
				if (fid2 != -1) {
					updateCapacityStatement.clearParameters();
					updateCapacityStatement.setInt(2, fid2);
					updateCapacityStatement.setInt(1, taken_seats2 + 1);
					updateCapacityStatement.execute();
				}
				
				commitTransaction();
				System.out.println("Reservation Successfully Booked!");
				break outer;
	
			} catch (SQLException e) {
				rollbackTransaction();
				attempts++;
			}
		}

		

		
	}

	public void transaction_reservations() throws Exception {
		if(!loggedIn) {
			System.out.println("Sorry you need to be logged in to see reservations");
			return;
		}
		
		boolean hasReservations = false;
		
		int attempts = 1;
		outer: while(true) {
			if(attempts > 20) {
				System.out.println("Transaction has failed: " + attempts + " times, aborting...");
			}
			try {
				beginTransaction();
				pullReservationsStatement.clearParameters();
				pullReservationsStatement.setString(1, this.username);
				ResultSet pullReserveResults = pullReservationsStatement.executeQuery();
				
				while (pullReserveResults.next()) {
					hasReservations = true;
					int fid1 = pullReserveResults.getInt("fid1");
					int fid2 = pullReserveResults.getInt("fid2");
					int rid = pullReserveResults.getInt("rid");
					
					pullFlightInfoStatement.clearParameters();
					pullFlightInfoStatement.setInt(1, fid1);
					ResultSet flightInfo = pullFlightInfoStatement.executeQuery();
					flightInfo.next();
					String cid = flightInfo.getString("carrier_id");
		            int fnum = flightInfo.getInt("flight_num");
		            String ocity = flightInfo.getString("origin_city");
		            String dcity = flightInfo.getString("dest_city");
		            int time = flightInfo.getInt("actual_time");
		            
		            System.out.println("Reservation " + rid);
		            printFlights(cid, fnum, ocity, dcity, time);
		            
		            if(fid2 != 0) {
		            	pullFlightInfoStatement.clearParameters();
		    			pullFlightInfoStatement.setInt(1, fid2);
		    			ResultSet flightInfo2 = pullFlightInfoStatement.executeQuery();
		    			flightInfo2.next();
		    			
		                String cid2 = flightInfo2.getString("carrier_id");
		                int fnum2 = flightInfo2.getInt("flight_num");
		                String ocity2 = flightInfo2.getString("origin_city");
		                String dcity2 = flightInfo2.getString("dest_city");
		                int time2 = flightInfo2.getInt("actual_time");
		                
		                printFlights(cid2, fnum2, ocity2, dcity2, time2);
		            }
		            System.out.println();
		
				}
				commitTransaction();
				pullReserveResults.close();
				break outer;
			} catch (SQLException e) {
				rollbackTransaction();
				attempts++;
			}
		}
		//error message for no reservations
		if(!hasReservations) {
			System.out.println("Sorry, you have no reservations");
		}
		
	}
	
	
	private void printFlights(String cid1, int fnum1, String ocity1, String dcity1, int min1) {
        System.out.println(cid1 + " Flight " + fnum1 + " from " + ocity1 + " to " + dcity1 + " (" + min1 + " minutes)");
	}

	public void transaction_cancel(int reservationId) throws Exception {
		//check if reservation exists
		if(!loggedIn) {
			System.out.println("Sorry you need to be logged in cancel reservations");
			return;
		}
		
		int attempts = 1;
		outer: while(true) {
			if(attempts > 20) {
				System.out.println("Transaction has failed: " + attempts + " times, aborting...");
			}
			try {
				beginTransaction();
				checkReservationStatement.clearParameters();
				checkReservationStatement.setInt(1, reservationId);
				checkReservationStatement.setString(2, username);
				ResultSet checkRes = checkReservationStatement.executeQuery();
				if(!checkRes.next()) {
					System.out.println("Sorry this reservation doesn't exist!");
					return;
				}
				checkRes.close();
				
				deleteReservationStatement.clearParameters();
				deleteReservationStatement.setInt(1, reservationId);
				deleteReservationStatement.setString(2, username); 
				deleteReservationStatement.execute();
				
				commitTransaction();
				System.out.println("Cancellation successful!");
				break outer;
			} catch (SQLException e) {
				rollbackTransaction();
				attempts++;
			}
		}
	}

    
       public void beginTransaction() throws Exception {
            conn.setAutoCommit(false);
            beginTransactionStatement.executeUpdate();  
        }

        public void commitTransaction() throws Exception {
            commitTransactionStatement.executeUpdate(); 
            conn.setAutoCommit(true);
        }
        public void rollbackTransaction() throws Exception {
            rollbackTransactionStatement.executeUpdate();
            conn.setAutoCommit(true);
            } 

}
