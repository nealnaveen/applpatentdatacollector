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
import java.net.URLEncoder;
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

public class RejectionLambda implements RequestHandler<Object, String> {


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


        Map<String, AttributeValue> item = getDynamoDBItem(ddb, "RejectionLambdaConfig", "ConfigName", "RejectionLambdaConfig");
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

                String apiUrl = "https://developer.uspto.gov/ds-api/oa_rejections/v2/records";
                // Prepare the URL-encoded form data
                String formData = "criteria=" + URLEncoder.encode("*:*", StandardCharsets.UTF_8.toString()) +
                        "&start=" + URLEncoder.encode(String.valueOf(startNumber), StandardCharsets.UTF_8.toString()) +
                        "&rows=" + URLEncoder.encode(String.valueOf(totalQuantity), StandardCharsets.UTF_8.toString());

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(apiUrl))
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .header("Accept", "application/json")
                        .header("Accept-Encoding", "gzip, deflate")
                        .POST(HttpRequest.BodyPublishers.ofString(formData))
                        .build();

                // Send the POST request
                HttpResponse response = client.send(request, gzipBodyHandler());
                System.out.println("Response status code: " + response.statusCode());

                //String responseString = new BasicResponseHandler().handleResponse(response);
                //System.out.println(response.body());
                // Parse JSON from the response body
                String content = response.body().toString();

                JsonNode rootNode = mapper.readTree(content);
                JsonNode results = rootNode.path("response").path("docs");

                if (results.isEmpty() ) {
                    recordsAvailable = false;
                } else {
                    // SQL statement for inserting data
                    String sql = "INSERT INTO rejections (id, patentApplicationNumber, obsoleteDocumentIdentifier, groupArtUnitNumber, legacyDocumentCodeIdentifier, submissionDate, nationalClass, " +
                            "nationalSubclass, headerMissing, formParagraphMissing, rejectFormMissmatch, closingMissing, hasRej101, hasRejDP, hasRej102, hasRej103, hasRej112, hasObjection, cite102GT1, " +
                            "cite103GT3, cite103EQ1, cite103Max, signatureType, actionTypeCategory, legalSectionCode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                    PreparedStatement pstmt = con.prepareStatement(sql);

                    // Iterate over the results array
                    for (JsonNode node : results) {
                        pstmt.setString(1, node.path("id").asText());
                        pstmt.setString(2, node.path("patentApplicationNumber").asText());
                        pstmt.setString(3, node.path("obsoleteDocumentIdentifier").asText());
                        pstmt.setInt(4, node.path("groupArtUnitNumber").asInt());
                        pstmt.setString(5, node.path("legacyDocumentCodeIdentifier").asText());
                        pstmt.setTimestamp(6, getSQLTimestamp(node.path("submissionDate").asText().replace("Z", "")));
                        pstmt.setInt(7, node.path("nationalClass").asInt());
                        pstmt.setString(8, node.path("nationalSubclass").asText());
                        pstmt.setBoolean(9, node.path("headerMissing").asBoolean());
                        pstmt.setBoolean(10, node.path("formParagraphMissing").asBoolean());
                        pstmt.setBoolean(11, node.path("rejectFormMissmatch").asBoolean());
                        pstmt.setBoolean(12, node.path("closingMissing").asBoolean());
                        pstmt.setBoolean(13, node.path("hasRej101").asBoolean());
                        pstmt.setBoolean(14, node.path("hasRejDP").asBoolean());
                        pstmt.setBoolean(15, node.path("hasRej102").asBoolean());
                        pstmt.setBoolean(16, node.path("hasRej103").asBoolean());
                        pstmt.setBoolean(17, node.path("hasRej112").asBoolean());
                        pstmt.setBoolean(18, node.path("hasObjection").asBoolean());
                        pstmt.setBoolean(19, node.path("cite102GT1").asBoolean());
                        pstmt.setBoolean(20, node.path("cite103GT3").asBoolean());
                        pstmt.setBoolean(21, node.path("cite103EQ1").asBoolean());
                        pstmt.setInt(22, node.path("cite103Max").asInt());
                        pstmt.setInt(23, node.path("signatureType").asInt());
                        pstmt.setString(24, node.path("actionTypeCategory").asText());
                        pstmt.setString(25, node.path("legalSectionCode").asText());
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
            key.put("ConfigName", AttributeValue.builder().s("RejectionLambdaConfig").build());

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

