// All JARS are loaded into the command line with jars/*: extension and all jars are in the "jars" folder
// Compile: javac -cp "jars/*:" streamcodeLoop.java 
// Run:     java -cp "jars/*:" streamcodeLoop
// javac -cp "jars/*:" streamcodeLoop.java && java -cp "jars/*:" streamcodeLoop 

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import java.time.LocalDateTime;
import java.util.Random;

class streamcodeLoop {

    // Path to the profile JSON containing Snowflake connection properties
    private static final String PROFILE_PATH = "profile.json";
    
    // ObjectMapper instance for reading JSON data
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        
        // Load connection properties from profile.json
        Properties props = new Properties();
        Iterator<Map.Entry<String, JsonNode>> propIt = mapper
            .readTree(new String(Files.readAllBytes(Paths.get(PROFILE_PATH))))
            .fields();
        
        // Populate properties from JSON
        while (propIt.hasNext()) {
            Map.Entry<String, JsonNode> prop = propIt.next();
            props.put(prop.getKey(), prop.getValue().asText());
        }

        // Counter to track the number of runs
        int run = 0;

        // Loop to run for an hour
        for (long stop = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(3600); stop > System.nanoTime();) {

            // Measure the start time for each loop iteration
            Long start = System.currentTimeMillis();

            // Using try-with-resources to ensure the Snowflake client is closed properly
            try (
                // Create a streaming ingest client
                SnowflakeStreamingIngestClient client = 
                    SnowflakeStreamingIngestClientFactory.builder("STREAM_CLIENT")
                    .setProperties(props)
                    .build()
            ) {

                // Build a request to open a channel for data ingestion
                OpenChannelRequest request1 = OpenChannelRequest.builder("MY_CHANNEL")
                    .setDBName("streaming")       // Database name
                    .setSchemaName("public")      // Schema name
                    .setTableName("streamtable")  // Target table name
                    .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE) // Continue on error (alternative: ABORT)
                    .build();

                // Open a streaming ingest channel
                SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);

                // Insert multiple rows into the channel (using insertRows API)
                final int totalRowsInTable = 100;
                run++;  // Increment the run counter

                // Loop to insert 'totalRowsInTable' rows
                for (int val = 0; val < totalRowsInTable; val++) {
                    // Create a row to insert
                    Map<String, Object> row = new HashMap<>();
                    row.put("A", randomString(10));       // Random 10-character string for column A
                    row.put("B", LocalDateTime.now());    // Current timestamp for column B
                    row.put("C", val);                    // Integer value for column C
                    row.put("D", randomString(6));        // Random 6-character string for column D
                    row.put("E", run);                    // Run count for column E
                    
                    // Insert the row with the current offset_token
                    InsertValidationResponse response = channel1.insertRow(row, String.valueOf(val));
                }

                // Close the channel once all rows are inserted
                channel1.close().get();
            }

            // Measure the end time and print the duration for the current iteration
            Long end = System.currentTimeMillis();
            System.out.println("------------------------------");
            System.out.println("Time taken: " + (end - start) + " ms");

        } // End of hourly loop

    } // End of main method

    // Helper method to generate a random string of a specified length
    public static String randomString(int targetStringLength) {
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'
        Random random = new Random();

        // Generate a random string by filtering between numerals and letters
        String generatedString = random.ints(leftLimit, rightLimit + 1)
            .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97)) // Include only digits and letters
            .limit(targetStringLength) // Limit to the target string length
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();

        return generatedString;
    }

} // End of streamcodeLoop class
