package week1exercise.tables;

import week1exercise.ConnectionManager;
import week1exercise.beans.Quote;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Class to manage data and perform queries with database
 */
public class QuotesManager {

    // Singleton instance of connection
    private static Connection conn = ConnectionManager.getInstance().getConnection();

    /**
     * Executes SQL command to clear all data in database
     */
    public static void clearDB() {
        String sql = "TRUNCATE TABLE quotes";
        try(
                PreparedStatement stmt = conn.prepareStatement(sql);
                ) {
            stmt.executeQuery();
        } catch(SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Database cleared");
    }

    /**
     * Insert Quote object into database
     * @param bean Quote object to insert
     * @return true if insert was successful, otherwise false
     * @throws ParseException
     */
    public static boolean insert(Quote bean) throws ParseException {
        String sql = "INSERT into quotes (symbol, price, volume, date, time) " + "VALUES (?,?,?,?,?)";

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        java.util.Date date = sdf.parse(bean.getDate());
        java.sql.Date sqlDate = new java.sql.Date(date.getTime());

        SimpleDateFormat stf = new SimpleDateFormat("kk:mm:ss");
        java.util.Date time = stf.parse(bean.getTime());
        java.sql.Time sqlTime = new java.sql.Time(time.getTime());

        try (
                PreparedStatement stmt = conn.prepareStatement(sql);
                ) {
            stmt.setString(1, bean.getSymbol());
            stmt.setDouble(2, bean.getPrice());
            stmt.setInt(3, bean.getVolume());
            stmt.setDate(4, sqlDate);
            stmt.setTime(5, sqlTime);

            int affected = stmt.executeUpdate();
            if(affected == 1) {
                return true;
            } else {
                return false;
            }
        } catch(SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Queries database for closing price given a symbol and date
     * @param sym Stock symbol to query
     * @param date Date to query
     * @return Quote object with query results, null if query was unsuccessful
     */
    public static Quote searchClosingPrice(String sym, String date) {
        String sql = "SELECT * FROM quotes WHERE time = " +
                "(SELECT MAX(time) FROM quotes WHERE date = ? AND symbol = ?) " +
                "AND date = ? AND symbol = ?";
        ResultSet rs;

        try (
                PreparedStatement stmt = conn.prepareStatement(sql);
        ) {
            stmt.setString(1, date);
            stmt.setString(2, sym);
            stmt.setString(3, date);
            stmt.setString(4, sym);
            rs = stmt.executeQuery();
            Quote bean = new Quote();
            if(rs.next()) {
                bean.setSymbol(rs.getString("symbol"));
                bean.setPrice(rs.getDouble("price"));
                bean.setVolume(rs.getInt("volume"));
                bean.setDate(rs.getString("date"));
                bean.setTime(rs.getString("time"));
            } else {
                rs.close();
                return null;
            }

            rs.close();
            return bean;

        } catch(SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Queries database for total volume traded on a given date for a specific stock
     * @param sym Stock symbol to query
     * @param date Date to query
     * @return Total volume traded on specified date
     */
    public static int searchTotalVolume(String sym, String date) {
        String sql = "SELECT SUM(volume) AS totalVolume FROM quotes WHERE date = ? AND symbol = ?";

        return getVolume(sym, date, sql);
    }

    /**
     * Queries database for total volume traded during a given month for a specific stock
     * @param sym Stock symbol to query
     * @param date Date to query
     * @return Total volume traded during given month
     */
    public static int searchMonthlyVolume(String sym, String date) {
        String sql = "SELECT SUM(volume) AS totalVolume FROM quotes WHERE date LIKE ? AND symbol = ?";
        date = date.substring(0, date.length()-2).concat("%");

        return getVolume(sym, date, sql);
    }


    /**
     * Queries database for highest price in a given date for a specific stock
     * @param sym Stock symbol to query
     * @param date Date to query
     * @return Quote object with query results, null if unsuccessful
     */
    public static Quote searchHighestPrice(String sym, String date) {
        String sql = "SELECT * FROM quotes WHERE price = " +
                "(SELECT MAX(price) FROM quotes WHERE date = ? AND symbol = ?)";

        return getBean(sym, date, sql);
    }

    /**
     * Queries database for lowest price in a given date for a specific stock
     * @param sym Stock symbol to query
     * @param date Date to query
     * @return Quote object with query results, null if unsuccessful
     */
    public static Quote searchLowestPrice(String sym, String date) {
        String sql = "SELECT * FROM quotes WHERE price = " +
                "(SELECT MIN(price) FROM quotes WHERE date = ? AND symbol = ?)";

        return getBean(sym, date, sql);
    }

    /**
     * Queries database for highest price during a given month for a specific stock
     * @param sym Stock symbol to query
     * @param date Date to query
     * @return Quote object with query results, null if unsuccessful
     */
    public static Quote searchHighestMonthly(String sym, String date) {
        String sql = "SELECT * FROM quotes WHERE price = (SELECT MAX(price) FROM quotes WHERE date LIKE ? AND symbol = ?)";
        date = date.substring(0, date.length()-2).concat("%");
        return getBean(sym, date, sql);
    }

    /**
     * Queries database for lowest price during a given month for a specific stock
     * @param sym Stock symbol to query
     * @param date Date to query
     * @return Quote object with query results, null if unsuccessful
     */
    public static Quote searchLowestMonthly(String sym, String date) {
        String sql = "SELECT * FROM quotes WHERE price = (SELECT MIN(price) FROM quotes WHERE date LIKE ? AND symbol = ?)";
        date = date.substring(0, date.length()-2).concat("%");
        return getBean(sym, date, sql);
    }

    /**
     * Helper function to query database and calculate total volume during a given time period for a specific stock
     * @param sym Stock symbol to query
     * @param date Date to query
     * @param sql SQL command to execute
     * @return Total volume for specified stock in specified time-frame
     */
    private static int getVolume(String sym, String date, String sql) {
        ResultSet rs;
        try (
                PreparedStatement stmt = conn.prepareStatement(sql);
        ) {
            stmt.setString(1, date);
            stmt.setString(2, sym);
            rs = stmt.executeQuery();
            if(rs.next()) {
                int totalVol = rs.getInt("totalVolume");
                rs.close();
                return totalVol;
            } else {
                return -1;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }
    }

    /**
     * Helper function to query database and parse results into a Quote object
     * @param sym Stock symbol to query
     * @param date Date to query
     * @param sql SQL command to execute
     * @return Query results represented as a Quote object
     */
    private static Quote getBean(String sym, String date, String sql) {
        try(
                PreparedStatement stmt = conn.prepareStatement(sql);
                ) {

            ResultSet rs;
            stmt.setString(1, date);
            stmt.setString(2, sym);
            rs = stmt.executeQuery();
            Quote bean;
            if(rs.next()) {
                bean = new Quote();
                bean.setSymbol(rs.getString("symbol"));
                bean.setPrice(rs.getDouble("price"));
                bean.setVolume(rs.getInt("volume"));
                bean.setDate(rs.getString("date"));
                bean.setTime(rs.getString("time"));
            } else {
                rs.close();
                return null;
            }
            rs.close();
            return bean;

        } catch(SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
}
