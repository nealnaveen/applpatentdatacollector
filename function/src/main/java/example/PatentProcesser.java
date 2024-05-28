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
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rdsdata.RdsDataClient;
import software.amazon.awssdk.services.rdsdata.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;


import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;


public class ProcessS3AppPatentEventLambda implements RequestHandler<S3Event, String> {
    private static final String DB_CLUSTER_ARN = "arn:aws:rds:us-east-1:508582898882:cluster:datacollector";
    private static final String DB_CREDENTIALS_ARN = "arn:aws:secretsmanager:us-east-1:508582898882:secret:rds!cluster-ce583560-543c-4698-81c0-e3a91caf27f1-S0lGc5";
    private static final String DB_NAME = "postgres";

    @Override
    public String handleRequest(S3Event s3event, Context context) {
       LambdaLogger logger = context.getLogger();
        S3EventNotificationRecord record = s3event.getRecords().get(0);
        String srcBucket = record.getS3().getBucket().getName();
        String srcKey = record.getS3().getObject().getUrlDecodedKey();
        Path localPath = Paths.get("/tmp", srcKey);
        int limit = 1;

        S3Client s3Client = S3Client.builder().build();

        // Download the file to /tmp directory
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(srcBucket)
                .key(srcKey)
                .build();
        s3Client.getObject(getObjectRequest, ResponseTransformer.toFile(localPath));

        // Log download completion and create File object
        System.out.println("Download complete. File saved to /tmp directory.");

        RdsDataClient rdsDataClient = RdsDataClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .build();

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
                    filters.addRule(new SuffixFilter("xml"));
                    dumpReader.setFileFilter(filters);
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
                    String inventors = mapper.writeValueAsString(patent.getInventors());
                    String applicants = mapper.writeValueAsString(patent.getApplicants());
                    String assignees = mapper.writeValueAsString(patent.getAssignee());
                    String continuity = getContinuity(patent.getRelationIds())
                    Date sqlFilingDate = Date.valueOf(filingDate);

                    Map<String, SqlParameter> params = new HashMap<>();
                    params.put("applicationId", param("applicationId", applicationId));
                    params.put("filingDate", param("filingDate", sqlFilingDate.toString(), TypeHint.DATE));
                    params.put("inventionTitle", param("inventionTitle", inventionTitle));
                    params.put("inventors",  param("inventors", filterParties(inventors), TypeHint.JSON));
                    params.put("applicants", param("applicants", filterParties(applicants), TypeHint.JSON));
                    params.put("assignees",  param("assignees", filterParties(assignees), TypeHint.JSON));


                    String sql = "INSERT INTO applications (applicationId, filingDate, inventionTitle, inventors, " +
                            "applicants, assignees) VALUES (:applicationId, :filingDate, :inventionTitle, :inventors, " +
                            ":applicants, :assignees)";
                    ExecuteStatementRequest request = ExecuteStatementRequest.builder()
                            .secretArn(DB_CREDENTIALS_ARN)
                            .resourceArn(DB_CLUSTER_ARN)
                            .database(DB_NAME)
                            .sql(sql)
                            .parameters(params.values())
                            .build();
                    ExecuteStatementResponse response = rdsDataClient.executeStatement(request);

                    System.out.println("Rows affected: " + response.numberOfRecordsUpdated());
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


    static SqlParameter param(String name, String value, TypeHint typeHint) {

        return SqlParameter.builder().typeHint(typeHint).name(name).value(Field.builder().stringValue(value).build()).build();

    }

    static SqlParameter param(String name, String value) {

        return SqlParameter.builder().name(name).value(Field.builder().stringValue(value).build()).build();
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

