package example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

public class PtabLambda implements RequestHandler<Object, String> {


    @Override
    public String handleRequest(Object input, Context context) {
        String url = "jdbc:postgresql://patentsdb.cto8wsaak48e.us-east-2.rds.amazonaws.com:5432/postgres";
        String user = "ptodev";
        String password = "Gia$2013";

        ObjectMapper mapper = new ObjectMapper();
        HttpClient client = HttpClient.newHttpClient();

        int startNumber = 0;
        int totalQuantity = 0;
        boolean recordsAvailable = true;
        Region region = Region.US_EAST_2;
        DynamoDbClient ddb = DynamoDbClient.builder()
                .region(region)
                .build();


        Map<String, AttributeValue> item = getDynamoDBItem(ddb, "RejectionLambdaConfig", "ConfigName", "PTABLambdaConfig");
        if (item != null && item.containsKey("StartNumber") && item.containsKey("TotalQuantity")) {
            startNumber = Integer.parseInt(item.get("StartNumber").n());
            totalQuantity = Integer.parseInt(item.get("TotalQuantity").n());
        }
        else{
            throw new RuntimeException("No DDB entry");
        }
        System.out.print("startNumber = " + startNumber);
        try (Connection con = DriverManager.getConnection(url, user, password)) {
            while (recordsAvailable) {
                // Read startNumber and totalQuantity from DynamoDB

                String apiUrl = "https://developer.uspto.gov/ptab-api/proceedings?recordTotalQuantity=" + totalQuantity + "&recordStartNumber=" + startNumber;
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(apiUrl))
                        .build();

                // Send HTTP Request and Receive JSON Response
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                System.out.println("Response status code: " + response.statusCode());

               // Parse JSON from the response body
                JsonNode rootNode = mapper.readTree(response.body());
                JsonNode results = rootNode.path("results");

                if (results.isEmpty()) {
                    recordsAvailable = false;
                } else {
                    // SQL statement for inserting data
                    String sql = "INSERT INTO proceeding (proceedingFilingDate, proceedingStatusCategory, proceedingNumber, proceedingLastModifiedDate, " +
                            "proceedingTypeCategory, subproceedingTypeCategory, respondentTechnologyCenterNumber, respondentPartyName, " +
                            "respondentGroupArtUnitNumber, respondentApplicationNumberText, decisionDate, appellantTechnologyCenterNumber, " +
                            "appellantPatentOwnerName, appellantPartyName, appellantGroupArtUnitNumber, appellantInventorName, " +
                            "appellantCounselName, appellantApplicationNumberText, additionalRespondentPartyDataBag) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                    PreparedStatement pstmt = con.prepareStatement(sql);

                    // Iterate over the results array
                    for (JsonNode node : results) {
                        java.sql.Date proceedingFilingDate = node.path("proceedingFilingDate").asText() != null && !node.path("proceedingFilingDate").asText().isEmpty() ? getSQlDate(node.path("proceedingFilingDate").asText()) : null;
                        pstmt.setDate(1, proceedingFilingDate);
                        pstmt.setString(2, node.path("proceedingStatusCategory").asText());
                        pstmt.setInt(3, node.path("proceedingNumber").asInt());
                        pstmt.setDate(4, node.path("proceedingFilingDate").asText() != null && !node.path("proceedingLastModifiedDate").asText().isEmpty() ? getSQlDate(node.path("proceedingLastModifiedDate").asText()) : null);
                        pstmt.setString(5, node.path("proceedingTypeCategory").asText());
                        pstmt.setString(6, node.path("subproceedingTypeCategory").asText());
                        pstmt.setInt(7, node.path("respondentTechnologyCenterNumber").asInt());
                        pstmt.setString(8, node.path("respondentPartyName").asText());
                        pstmt.setInt(9, node.path("respondentGroupArtUnitNumber").asInt());
                        pstmt.setString(10, node.path("respondentApplicationNumberText").asText());
                        pstmt.setDate(11, node.path("proceedingFilingDate").asText() != null && !node.path("decisionDate").asText().isEmpty() ? getSQlDate(node.path("decisionDate").asText()) : null);
                        pstmt.setInt(12, node.path("appellantTechnologyCenterNumber").asInt());
                        pstmt.setString(13, node.path("appellantPatentOwnerName").asText());
                        pstmt.setString(14, node.path("appellantPartyName").asText());
                        pstmt.setInt(15, node.path("appellantGroupArtUnitNumber").asInt());
                        pstmt.setString(16, node.path("appellantInventorName").asText());
                        pstmt.setString(17, node.path("appellantCounselName").asText());
                        pstmt.setString(18, node.path("appellantApplicationNumberText").asText());
                        pstmt.setArray(19, con.createArrayOf("text", new String[]{}));  // Assuming empty array
                        pstmt.executeUpdate();
                    }
                    startNumber += totalQuantity;
                    System.out.print("Records inserted" +totalQuantity);
                }
            }

            // Update startNumber in DynamoDB
            Map<String, AttributeValueUpdate> updates = new HashMap<>();
            updates.put("StartNumber", AttributeValueUpdate.builder().value(AttributeValue.builder().n(Integer.toString(startNumber )).build()).build());
            Map<String, AttributeValue> key = new HashMap<>();
            key.put("ConfigName", AttributeValue.builder().s("PTABLambdaConfig").build());

            UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
                    .tableName("RejectionLambdaConfig")
                    .key(key)
                    .attributeUpdates(updates)
                    .build();

            ddb.updateItem(updateItemRequest);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            ddb.close();
        }
        return "Success";
    }


    private static java.sql.Date getSQlDate (String date){
        java.sql.Date sqlDate = null;
        try {
            // Define the format of the input date string
            SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy");
            // Parse the string into java.util.Date
            java.util.Date utilDate = sdf.parse(date);
            // Convert java.util.Date to java.sql.Date
            sqlDate = new java.sql.Date(utilDate.getTime());

            // Output the converted date
            System.out.println("Converted java.sql.Date: " + sqlDate);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return sqlDate;
    }

    private static  java.sql.Timestamp getSQLTimestamp(String timestampStr) {
        DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
        LocalDateTime dateTime = LocalDateTime.parse(timestampStr, formatter);
        return Timestamp.valueOf(dateTime);
    }

    public static Map<String, AttributeValue>  getDynamoDBItem(DynamoDbClient ddb, String tableName, String key, String keyVal) {
        HashMap<String, AttributeValue> keyToGet = new HashMap<>();
        Map<String, AttributeValue> returnedItem = null;
        keyToGet.put(key, AttributeValue.builder()
                .s(keyVal)
                .build());

        GetItemRequest request = GetItemRequest.builder()
                .key(keyToGet)
                .tableName(tableName)
                .build();

        try {
            // If there is no matching item, GetItem does not return any data.
            returnedItem = ddb.getItem(request).item();
            if (returnedItem.isEmpty())
                System.out.format("No item found with the key %s!\n", key);
            else {
                Set<String> keys = returnedItem.keySet();
                System.out.println("Amazon DynamoDB table attributes: \n");
                for (String key1 : keys) {
                    System.out.format("%s: %s\n", key1, returnedItem.get(key1).toString());
                }
            }

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return returnedItem;
    }

    public static HttpResponse.BodyHandler<String> gzipBodyHandler() {
        return responseInfo -> {
            HttpResponse.BodySubscriber<InputStream> original = HttpResponse.BodySubscribers.ofInputStream();
            return HttpResponse.BodySubscribers.mapping(
                    original,
                    inputStream -> {
                        if ("gzip".equalsIgnoreCase(responseInfo.headers().firstValue("Content-Encoding").orElse(""))) {
                            try (GZIPInputStream gis = new GZIPInputStream(inputStream)) {
                                return new String(gis.readAllBytes(), StandardCharsets.UTF_8);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        } else {
                            try {
                                return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
            );
        };
    }
}


