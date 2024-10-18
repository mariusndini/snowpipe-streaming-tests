// All JARS are loaded into the command line w/ jars/*: extension and all jars are in the "jars" folder
//  Compile: javac -cp "jars/*:" streamcode.java 
//  Run:     java -cp "jars/*:" streamcode  
// javac -cp "jars/*:" streamcode.java && java -cp "jars/*:" streamcode


// https://javadoc.io/doc/net.snowflake/snowflake-ingest-sdk/latest/index.html
// https://github.com/snowflakedb/snowflake-ingest-java

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


class streamcode {

    // Path to the profile JSON containing connection properties
    private static final String PROFILE_PATH = "profile.json";
    
    // ObjectMapper instance to parse JSON files
    private static final ObjectMapper mapper = new ObjectMapper();

    // Main method that drives the Snowpipe Streaming ingestion process
    public static void main(String[] args) throws Exception {
        
        // Load connection properties from profile.json
        Properties props = new Properties();
        Iterator<Map.Entry<String, JsonNode>> propIt = mapper
            .readTree(new String(Files.readAllBytes(Paths.get(PROFILE_PATH))))
            .fields();
        
        // Iterate over the properties in the profile JSON and load them into 'props'
        while (propIt.hasNext()) {
            Map.Entry<String, JsonNode> prop = propIt.next();
            props.put(prop.getKey(), prop.getValue().asText());
        }

    
        // Using try-with-resources to ensure the Snowflake client is closed automatically
        try (
            // Create a streaming ingest client
            SnowflakeStreamingIngestClient client = 
                SnowflakeStreamingIngestClientFactory.builder("STREAM_CLIENT")
                .setProperties(props)
                .build()
        ) {

            Long start = System.currentTimeMillis();
            // Build a request to open a channel to stream data into Snowflake
            OpenChannelRequest request1 = OpenChannelRequest.builder("MY_CHANNEL")
                .setDBName("streaming")    // Database name
                .setSchemaName("public")    // Schema name
                .setTableName("streamtable") // Target table name
                .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE) // Continue on error (alternative: ABORT)
                .build();

            // Open a streaming ingest channel using the request
            SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);

            // Prepare the first row of data to be streamed
            Map<String, Object> row = new HashMap<>();
            row.put("A",  "NO ERROR" );          // Random 10-character string for column A
            row.put("B", LocalDateTime.now());       // Current timestamp for column B
            row.put("C", 1);                         // Integer for column C
            row.put("D", randomString(6));           // Random 6-character string for column D
            row.put("E", 1);                         // Integer for column E
            
            // Insert the row into Snowflake and validate the response
            InsertValidationResponse response = channel1.insertRow(row, String.valueOf(1));

            Long end = System.currentTimeMillis();

            System.out.println("------------------------------");
            System.out.println("------------------------------");
            System.out.println("Time taken: " + (end - start) + " ms");
            System.out.println("ARE THERE ANY ERRORS: " + response.hasErrors() + ". IF SO HOW MANY: " + response.getErrorRowCount());
            System.out.print(response.getInsertErrors());
            System.out.println("\n------------------------------");
            System.out.println("------------------------------");
  

//------------------------------------------------------------------------------------------------------------------------------------------------------------------
            // ERROR - SEE ERRORS ON INSERT
            // Now attempt to insert another row which will cause an error due to incorrect column types
            // (e.g., using an int key instead of a string in 'row.put(123, 1)')
            Map<String, Object> errorRow = new HashMap<>();
            errorRow.put("A", "ERROR");      // Random 10-character string for column A
            errorRow.put("B", LocalDateTime.now());   // Current timestamp for column B
            errorRow.put("C", 2);                     // Integer for column C
            errorRow.put("D", randomString(6));       // Random 6-character string for column D
            errorRow.put("E", "ABC");                     // This will cause an error because the key should be a int
            
            // Insert the row and expect an error due to the invalid key type
            response = channel1.insertRow(errorRow, String.valueOf(2));

            System.out.println("\n------------------------------");
            System.out.println("------------------------------");
            System.out.println("ARE THERE ANY ERRORS: " + response.hasErrors() + ". IF SO HOW MANY: " + response.getErrorRowCount());
            System.out.print(response.getInsertErrors());
            // if(response.hasErrors()){
            //     throw response.getInsertErrors().get(0).getException();
            // }

            System.out.println("\n------------------------------");
            System.out.println("------------------------------");


            // Close the channel after all rows have been inserted
            channel1.close().get();
        }

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

} // End of streamcode class
