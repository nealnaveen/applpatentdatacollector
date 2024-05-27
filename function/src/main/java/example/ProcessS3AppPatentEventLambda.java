package example;


import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import gov.uspto.common.filter.FileFilterChain;
import gov.uspto.common.filter.SuffixFilter;
import gov.uspto.patent.PatentDocFormat;
import gov.uspto.patent.PatentDocFormatDetect;
import gov.uspto.patent.PatentReader;
import gov.uspto.patent.bulk.DumpFileAps;
import gov.uspto.patent.bulk.DumpFileXml;
import gov.uspto.patent.bulk.DumpReader;
import gov.uspto.patent.model.Patent;
import org.postgresql.util.PGobject;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;


import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;


public class ProcessS3AppPatentEventLambda implements RequestHandler<S3Event, String> {
    @Override
    public String handleRequest(S3Event s3event, Context context) {
        LambdaLogger logger = context.getLogger();
        S3EventNotificationRecord record = s3event.getRecords().get(0);
        String srcBucket = record.getS3().getBucket().getName();
        String srcKey = record.getS3().getObject().getUrlDecodedKey();
        Path localPath = Paths.get("/tmp", srcKey);
        int limit = 1;
        String url = "jdbc:postgresql://patentsdb.cto8wsaak48e.us-east-2.rds.amazonaws.com:5432/postgres";
        String user = "ptodev";
        String password = "Gia$2013";
        Connection conn = null;

        S3Client s3Client = S3Client.builder().build();

        // Download the file to /tmp directory
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(srcBucket)
                .key(srcKey)
                .build();
        s3Client.getObject(getObjectRequest, ResponseTransformer.toFile(localPath));

        // Log download completion and create File object
        System.out.println("Download complete. File saved to /tmp directory.");

        try {
            conn = DriverManager.getConnection(url, user, password);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        if (conn == null) {
            System.out.println("connection is null");

        }
        else {
            System.out.println("connection is not null");
        }


        try {
            // Create a File object from the downloaded file
            File inputFile = localPath.toFile();
            // Perform operations on the File object as needed
            System.out.println("File object created: " + inputFile.getAbsolutePath());

            PatentDocFormat patentDocFormat = new PatentDocFormatDetect().fromFileName(inputFile);
            DumpReader dumpReader;
            switch (patentDocFormat) {
                case Greenbook:
                    dumpReader = new DumpFileAps(inputFile);
                    break;
                default:
                    dumpReader = new DumpFileXml(inputFile);
                    FileFilterChain filters = new FileFilterChain();
                    //filters.addRule(new PathFileFilter(""));
                    filters.addRule(new SuffixFilter("xml"));
                    dumpReader.setFileFilter(filters);
            }
            if (dumpReader == null) {
                System.out.println("dumpreader is null");

            }
            else {
                System.out.println("dumpreader is not null");
            }



            dumpReader.open();



            PatentReader patentReader = new PatentReader(patentDocFormat);

            for (int i = 1; dumpReader.hasNext() && i <= limit; i++) {
                String xmlDocStr = (String) dumpReader.next();


                try (StringReader rawText = new StringReader(xmlDocStr)) {
                    Patent patent = patentReader.read(rawText);
                    String applicationId = patent.getDocumentId().getDocNumber();
                    LocalDate filingDate = patent.getDocumentDate().getDate();
                    String inventionTitle = patent.getTitle();
                    ObjectMapper mapper = new ObjectMapper();
                    System.out.println("RAW: " + applicationId);
                    //Converting the Object to JSONString
                    String inventors = mapper.writeValueAsString(patent.getInventors());
                    String applicants = mapper.writeValueAsString(patent.getApplicants());
                    String assignees = mapper.writeValueAsString(patent.getAssignee());

                        System. out.println(filterParties(applicants));
                    PGobject jsonInventors = new PGobject();
                    jsonInventors.setType("json");
                    jsonInventors.setValue(filterParties(inventors));

                    PGobject jsonApplicants = new PGobject();
                    jsonApplicants.setType("json");
                    jsonApplicants.setValue(filterParties(applicants));

                    PGobject jsonAssignees = new PGobject();
                    jsonAssignees.setType("json");
                    jsonAssignees.setValue(filterParties(assignees));

                    // Prepare SQL statement
                    String sql = "INSERT INTO applications (applicationId, filingDate, inventionTitle, inventors, applicants, assignees) VALUES (?, ?, ?, ?, ?, ?)";
                    try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                        pstmt.setString(1, applicationId);
                        pstmt.setDate(2, java.sql.Date.valueOf(filingDate)); // assuming `patent.toString()` returns the patent data
                        pstmt.setString(3, inventionTitle);
                        pstmt.setObject(4, jsonInventors);
                        pstmt.setObject(5, jsonApplicants);
                        pstmt.setObject(6, jsonAssignees);
                        pstmt.executeUpdate();
                     } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }


                dumpReader.close();
            }
        } catch (Exception e) {
            logger.log("Exception " + e.getMessage());
        } finally {
            try {
                if (Files.exists(localPath)) {
                    Files.delete(localPath);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return "Ok";
    }


    private HeadObjectResponse getHeadObject(S3Client s3Client, String bucket, String key) {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        return s3Client.headObject(headObjectRequest);
    }


    public static String filterParties(String jsonArray) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode arrayNode = (ArrayNode) mapper.readTree(jsonArray);
        ArrayNode filteredArray = mapper.createArrayNode();

        for (JsonNode node : arrayNode) {
            ObjectNode objectNode = (ObjectNode) node;
            ObjectNode nameNode = (ObjectNode) objectNode.get("name");
            ObjectNode filteredNode = mapper.createObjectNode();
            // Check and assign 'entityType'
            JsonNode entityTypeNode = objectNode.get("entityType");
            if (entityTypeNode != null && !entityTypeNode.isNull()) {
                filteredNode.put("entityType", entityTypeNode.asText());
            } else {
                filteredNode.put("entityType", ""); // or any default value or handle differently
            }

// Check and assign 'firstName' from 'nameNode'
            JsonNode firstNameNode = nameNode.get("firstName");
            if (firstNameNode != null && !firstNameNode.isNull()) {
                filteredNode.put("firstName", firstNameNode.asText());
            } else {
                filteredNode.put("firstName", ""); // or any default value or handle differently
            }

// Check and assign 'lastName'
            JsonNode lastNameNode = nameNode.get("lastName");
            if (lastNameNode != null && !lastNameNode.isNull()) {
                filteredNode.put("lastName", lastNameNode.asText());
            } else {
                filteredNode.put("lastName", ""); // or any default value or handle differently
            }
            JsonNode middleNameNode = objectNode.get("middleName");
            if (middleNameNode != null) {
                filteredNode.set("middleName", middleNameNode);
            }
            filteredArray.add(filteredNode);
        }

        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(filteredArray);

    }
}
/*public class ProcessS3AppPatentEventLambda implements RequestHandler<S3Event, String> {

    @Override
    public String handleRequest(S3Event event, Context context) {
        // Define the local file path in the Lambda /tmp directory

       LambdaLogger logger = context.getLogger();
       logger.log("S# event occured");
       *//*
        S3EventNotificationRecord record = event.getRecords().get(0);
        String srcBucket = record.getS3().getBucket().getName();
        String srcKey = record.getS3().getObject().getUrlDecodedKey();
        Path localPath = Paths.get("/tmp", srcKey);

        // Create S3Client with the region
        Region region = Region.of(record.getAwsRegion()); // Dynamically fetching region
        S3Client s3Client = S3Client.builder()
                .region(region)
                .build();

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(srcBucket)
                .key(srcKey)
                .build();

        try (ResponseInputStream<GetObjectResponse> s3Input = s3Client.getObject(getObjectRequest)) {

            // Download the file to /tmp directory
            s3Client.getObject(getObjectRequest, ResponseTransformer.toFile(localPath));

            // Log download completion and create File object
            System.out.println("Download complete. File saved to /tmp directory.");

            // Create a File object from the downloaded file
            File downloadedFile = localPath.toFile();
            // Perform operations on the File object as needed
            System.out.println("File object created: " + downloadedFile.getAbsolutePath());
        } catch (Exception e) {
            logger.log(e.getMessage());
        }*//*
        return "success";
    }

    private void insertPatentData(Patent patent, Connection conn) throws SQLException, JsonProcessingException {
        String applicationId = patent.getDocumentId().getDocNumber();
        LocalDate filingDate = patent.getDocumentDate().getDate();
        String inventionTitle = patent.getTitle();
        ObjectMapper mapper = new ObjectMapper();

        String inventors = mapper.writeValueAsString(patent.getInventors());
        String applicants = mapper.writeValueAsString(patent.getApplicants());
        String assignees = mapper.writeValueAsString(patent.getAssignee());

        PGobject jsonInventors = new PGobject();
        jsonInventors.setType("json");
        jsonInventors.setValue(inventors);

        PGobject jsonApplicants = new PGobject();
        jsonApplicants.setType("json");
        jsonApplicants.setValue(applicants);

        PGobject jsonAssignees = new PGobject();
        jsonAssignees.setType("json");
        jsonAssignees.setValue(assignees);

        String sql = "INSERT INTO applications (applicationId, filingDate, inventionTitle, inventors, applicants, assignees) VALUES (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, applicationId);
            pstmt.setDate(2, java.sql.Date.valueOf(filingDate));
            pstmt.setString(3, inventionTitle);
            pstmt.setObject(4, jsonInventors);
            pstmt.setObject(5, jsonApplicants);
            pstmt.setObject(6, jsonAssignees);
            pstmt.executeUpdate();
        }
    }
}*/
