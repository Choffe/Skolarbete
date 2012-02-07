package dbtLab3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * Database is a class that specifies the interface to the movie database. Uses
 * JDBC and the MySQL Connector/J driver.
 */
public class Database {
	/**
	 * The database connection.
	 */
	private Connection conn;

	/**
	 * An SQL statement object.
	 */
	private Statement stmt;

	/**
	 * Create the database interface object. Connection to the database is
	 * performed later.
	 */
	public Database() {
		conn = null;
	}

	/**
	 * Open a connection to the database, using the specified user name and
	 * password.
	 * 
	 * @param userName
	 *            The user name.
	 * @param password
	 *            The user's password.
	 * @return true if the connection succeeded, false if the supplied user name
	 *         and password were not recognized. Returns false also if the JDBC
	 *         driver isn't found.
	 */
	public boolean openConnection(String userName, String password) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(
					"jdbc:mysql://puccini.cs.lth.se/" + userName, userName,
					password);
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * Close the connection to the database.
	 */
	public void closeConnection() {
		try {
			if (conn != null) {
				conn.close();
			}
		} catch (SQLException e) {
		}
		conn = null;
	}

	/**
	 * Check if the connection to the database has been established
	 * 
	 * @return true if the connection has been established
	 */
	public boolean isConnected() {
		return conn != null;
	}

	/* --- insert own code here --- */

	public ArrayList<String> getMovies(){
		String sql = "select name from movie;";
		PreparedStatement ps = null;
		ArrayList<String> result = new ArrayList<String>();
		try {
			ps = conn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				result.add(rs.getString("name"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}finally{
			try {
				ps.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		return result;
		
	}
	public ArrayList<String> getShowdates(String movieName){
		String sql = "select showdate from performance where moviename = ?";
		PreparedStatement ps = null;
		ArrayList<String> result = new ArrayList<String>();
		try {
			ps = conn.prepareStatement(sql);
			ps.setString(1, movieName);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				result.add(rs.getString("showdate"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}finally{
			try {
				ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return result;
	}
	
	//Haha refaktorisera lite eller???
	public Performance getPerformance(String movieName, String showdate){
		String sql = "select * from performance where moviename = ? and showdate= ?";
		String sql2 = "select * from theater where name = ?";
		PreparedStatement ps = null;
		PreparedStatement ps2 = null;
		Performance performance = new Performance();
		try {
			ps = conn.prepareStatement(sql);
			ps.setString(1, movieName);
			ps.setString(2, showdate);
			ResultSet rs = ps.executeQuery();
			rs.next();
			performance.setShowdate(rs.getString("showdate"));
			performance.setShowtime(rs.getString("showtime"));
			performance.setBookedSeats(rs.getInt("bookedSeats"));
			performance.setMovieName(rs.getString("moviename"));
			performance.setTheaterName(rs.getString("theatername"));
			
			
			ps2 = conn.prepareStatement(sql2);
			ps2.setString(1, rs.getString("theatername"));
			ResultSet rs2 = ps2.executeQuery();
			rs2.next();
			performance.setTotalSeats(rs2.getInt("nbrOfSeats"));
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}finally{
			try {
				ps.close();
				ps2.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return performance;
	}
	
	public boolean makeReservation(Performance p){
	
		String sql = "select bookedSeats,theatername from performance where moviename = ? and showdate= ?";
		String sql2 = "update performance set bookedSeats = bookedSeats + 1 where moviename = ? and showdate = ?;";
		String sql3 = "select nbrOfSeats from Theater where name = ?;";
		PreparedStatement ps = null;
		PreparedStatement ps2 = null;
		try {
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(sql);
			ps.setString(1, p.getMovieName());
			ps.setString(2, p.getShowdate());
			ResultSet rs = ps.executeQuery();
			rs.next();
			ps2 = conn.prepareStatement(sql3);
			ps2.setString(1, rs.getString("theatername"));
			ResultSet rs2 = ps2.executeQuery();
			rs2.next();
			if(rs2.getInt("nbrOfSeats") - rs.getInt("bookedSeats") > 0){
				ps = conn.prepareStatement(sql2);
				ps.setString(1, p.getMovieName());
				ps.setString(2, p.getShowdate());
				if(ps.executeUpdate() != 1){
					conn.rollback();
					return false;
				}
			}else{
				conn.rollback();
				return false;
			}
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}finally{
			try {
				ps.close();
				ps2.close();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return true;
	}
	
}
