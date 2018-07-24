package week1exercise;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Class for singleton instance of SQL connection
 */
public class ConnectionManager {
    private static ConnectionManager instance = null;

    private final String USERNAME = "dbuser";
    private final String PASSWORD = "dbpassword";
    private final String H_CONN_STRING =
            "jdbc:hsqldb:data/myquotes";
    private final String M_CONN_STRING =
            "jdbc:mysql://localhost/myquotes";

    private DBType dbType = DBType.MYSQL;
    private Connection conn = null;

    /**
     * Private constructor to maintain singleton instantiation
     */
    private ConnectionManager() {
    }

    /**
     * Gets instance of singleton ConnectionManager. If singleton has not been instantiated, then it gets created
     * @return Singleton instantiation of ConnectionManager
     */
    public static ConnectionManager getInstance() {
        if (instance == null) {
            instance = new ConnectionManager();
        }
        return instance;
    }

    /**
     * Sets type of SQL database for connection purposes based on enumerations of databases in DBType class
     * @param dbType Type of database adhering to enumeration in DBType class
     */
    public void setDBType(DBType dbType) {
        this.dbType = dbType;
    }

    /**
     * Helper function to open connection to SQL database based on database type
     * @return true if connection was successful, otherwise false
     */
    private boolean openConnection()
    {
        try {
            switch (dbType) {

                case MYSQL:
                    conn = DriverManager.getConnection(M_CONN_STRING, USERNAME, PASSWORD);
                    return true;

                case HSQLDB:
                    conn = DriverManager.getConnection(H_CONN_STRING, USERNAME, PASSWORD);
                    return true;

                default:
                    return false;
            }
        }
        catch (SQLException e) {
            System.err.println(e);
            return false;
        }

    }

    /**
     * Gets the Connection object associated with singleton ConnectionManager
     * @return SQL Connection object if connection is open, otherwise null
     */
    public Connection getConnection()
    {
        if (conn == null) {
            if (openConnection()) {
                System.out.println("Connection opened\n");
                return conn;
            } else {
                return null;
            }
        }
        return conn;
    }

    /**
     * Closes connection object associated with singleton ConnectionManager
     */
    public void close() {
        System.out.println("\nClosing connection");
        try {
            conn.close();
            conn = null;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}