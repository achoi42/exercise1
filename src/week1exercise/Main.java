package week1exercise;

import week1exercise.beans.Quote;
import week1exercise.tables.QuotesManager;
import week1exercise.util.InputHelper;

import java.io.IOException;
import java.text.ParseException;

/**
 * Driver class for package. Contains main method to run, collects user input and queries database accordingly
 */
public class Main {
    // Given URL to database
    private static final String DBURL = "https://bootcamp-training-files.cfapps.io/week1/week1-stocks.json";

    /**
     * Main driver method to run program and query database
     * @param args Arguments for main executable
     * @throws IOException, ParseException
     */
    public static void main(String[] args) throws IOException, ParseException {

        // Open connection
        ConnectionManager.getInstance().setDBType(DBType.MYSQL);

        // Retrieve JSON data from URL as String
        String response = InputHelper.getDBFromURL(DBURL);
        QuotesManager.clearDB();
        // Parse JSON string into Quote objects and store in database
        InputHelper.parseJsonToBeans(response);

        boolean valid = false;
        while(!valid) {
            // Collect user input
            String myDate = InputHelper.getInput("Enter a date in YYYY-MM-DD format: ");
            String mySym = InputHelper.getInput("Enter a stock symbol: ").toUpperCase();

            // Queries database and stores results in Quote objects or int values
            Quote maxBean = QuotesManager.searchHighestPrice(mySym, myDate);
            Quote minBean = QuotesManager.searchLowestPrice(mySym, myDate);
            Quote closingBean = QuotesManager.searchClosingPrice(mySym, myDate);
            Quote monthMaxBean = QuotesManager.searchHighestMonthly(mySym, myDate);
            Quote monthMinBean = QuotesManager.searchLowestMonthly(mySym, myDate);
            int totalVol = QuotesManager.searchTotalVolume(mySym, myDate);
            int monthlyVol = QuotesManager.searchMonthlyVolume(mySym, myDate);

            if(maxBean == null || minBean == null || closingBean == null || monthMaxBean == null || monthMinBean == null || totalVol == -1) {
                System.out.println("\nQuote not found, try again\n");
                continue;
            }

            System.out.println("\n" + mySym + ", " + myDate + "\n");
            System.out.println("Highest stock price for " + mySym + " on " + myDate + ": " + maxBean.getPrice());
            System.out.println("Lowest stock price for " + mySym + " on " + myDate + ": " + minBean.getPrice());
            System.out.println("Total volume traded for " + mySym + " on " + myDate + ": " + totalVol);
            System.out.println("Closing price for " + mySym + " on " + myDate + ": " + closingBean.getPrice());
            System.out.println("Highest price of month: " + monthMaxBean.getPrice());
            System.out.println("Lowest price of month: " + monthMinBean.getPrice());
            System.out.println("Volume of month: " + monthlyVol);

            valid = true;
        }

        // Close connection
        ConnectionManager.getInstance().close();
    }
}