// All JARS are loaded into the command line w/ jars/*: extention and all jars are in the "jars" folder
//  javac -cp "jars/*:" streamcode.java 
//  java -cp "jars/*:" streamcode  


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
import java.time.*;
import java.util.*;


class streamcode {
    private static String PROFILE_PATH = "profile.json";
    private static final ObjectMapper mapper = new ObjectMapper();


    public static void main(String[] args) throws Exception {
    
        Properties props = new Properties();
        Iterator<Map.Entry<String, JsonNode>> propIt = mapper.readTree(new String(Files.readAllBytes(Paths.get(PROFILE_PATH)))).fields();
        
        while (propIt.hasNext()) {
            Map.Entry<String, JsonNode> prop = propIt.next();
            props.put(prop.getKey(), prop.getValue().asText());
        }


        int run = 0;
        for (long stop=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(3600);stop>System.nanoTime();) {

        

        Long start = System.currentTimeMillis();
        try ( // Create a streaming ingest client
            SnowflakeStreamingIngestClient client =
            SnowflakeStreamingIngestClientFactory.builder("STREAM_CLIENT").setProperties(props).build()) {


            OpenChannelRequest request1 =
                OpenChannelRequest.builder("MY_CHANNEL")
                    .setDBName("anondb")
                    .setSchemaName("public")
                    .setTableName("streamtable")
                    .setOnErrorOption(
                        OpenChannelRequest.OnErrorOption.CONTINUE) // Another ON_ERROR option is ABORT
                    .build();

            SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1); // Open a streaming ingest channel from the given client

            // Insert rows into the channel (Using insertRows API)
            final int totalRowsInTable = 10000000;
            run++;
            for (int val = 0; val < totalRowsInTable; val++) {
                Map<String, Object> row = new HashMap<>();
                    // Col name (a, b) and value for column anme
                row.put("A", randomString(10));
                row.put("B", LocalDateTime.now());
                row.put("C", val);
                row.put("D", randomString(6));
                row.put("E", run);

                // Insert the row with the current offset_token
                InsertValidationResponse response = channel1.insertRow(row, String.valueOf(val));

                
            }
            channel1.close().get();

        }


        Long end = System.currentTimeMillis();
        System.out.println("------------------------------");
        System.out.println(end - start);

    }// end hourly loop


    }//end main


    public static String randomString(int targetStringLength) {
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'
        Random random = new Random();

        String generatedString = random.ints(leftLimit, rightLimit + 1)
        .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
        .limit(targetStringLength)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();

        return generatedString;
    }




}// end class


