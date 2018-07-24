package week1exercise.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import week1exercise.beans.Quote;
import week1exercise.tables.QuotesManager;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.text.ParseException;

/**
 * Utility class to encapsulate collecting user input and parsing JSON data from HTTP response
 */
public class InputHelper {

    /**
     * Prompts user to provide input and collects input stream
     * @param prompt Text to prompt the user for input
     * @return User input
     */
    public static String getInput(String prompt) {
        BufferedReader stdin = new BufferedReader(
                new InputStreamReader(System.in));

        System.out.print(prompt);
        System.out.flush();

        try {
            return stdin.readLine();
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }

    /**
     * HTTP client code to request data from uri
     * Uses Apache HTTPClient
     * @param url URL link to dataset
     * @return String representation of JSON response
     * @throws IOException
     */
    public static String getDBFromURL(String url) throws IOException {
        StringBuilder responseBody = new StringBuilder();

        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet httpget = new HttpGet(url);

            // Create a custom response handler
            ResponseHandler<String> responseHandler = new ResponseHandler<String>() {
                @Override
                public String handleResponse(final HttpResponse response) throws IOException {
                    int status = response.getStatusLine().getStatusCode();
                    if (status >= 200 && status < 300) {
                        HttpEntity entity = response.getEntity();
                        return entity != null ? EntityUtils.toString(entity) : null;
                    } else {
                        throw new ClientProtocolException("Unexpected response status: " + status);
                    }
                }

            };

            String httpResponse = httpclient.execute(httpget, responseHandler);
            responseBody.append(httpResponse);
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        }

        return responseBody.toString();
    }

    /**
     * Converts string representation of JSON data into Quote objects and inserts data in SQL database
     * @param jsonString String representation of JSON response from dataset URL
     * @throws ParseException
     */
    public static void parseJsonToBeans(String jsonString) throws ParseException {
        StringReader is = new StringReader(jsonString);
        JsonReader reader = Json.createReader(is);
        JsonArray arr = reader.readArray();
        reader.close();
        is.close();

        Quote bean = new Quote();
        String timestamp;
        int dateTimeSplitIdx = 0;

        System.out.print("Saving list to database... ");
        for(int i = 0; i < arr.size(); i++) {
            bean.setSymbol(arr.getJsonObject(i).getString("symbol"));
            bean.setPrice(arr.getJsonObject(i).getJsonNumber("price").doubleValue());
            bean.setVolume(arr.getJsonObject(i).getInt("volume"));
            timestamp = arr.getJsonObject(i).getString("date");
            dateTimeSplitIdx = timestamp.indexOf('T');
            bean.setDate(timestamp.substring(0, dateTimeSplitIdx));
            bean.setTime(timestamp.substring(dateTimeSplitIdx+1, timestamp.length()-1));
            if(!QuotesManager.insert(bean)) {
                System.err.println("\nError saving list, saved " + (i+1) + " out of " + arr.size() + " quotes\n");
                return;
            }
        }
        System.out.println("Done\n");
    }
}