package com.example.demo;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class SavePersonHandler implements RequestHandler<SQSEvent, PersonResponse> {
    private DynamoDB dynamoDb;

    private String TABLE_NAME = "persons";
    private String S3_NAME = "my-s3-test-task";
    private String S3_KEY = "s3.csv";


    public PersonResponse handleRequest(SQSEvent event, Context context) {
        LambdaLogger logger = context.getLogger();
        this.initDynamoDbClient();

        Gson gson = new Gson();
        PersonRequest personRequest;

        try {
            for (SQSEvent.SQSMessage message : event.getRecords()) {
                String input = message.getBody();
                personRequest = gson.fromJson(input, PersonRequest.class);
                persistData(personRequest);
            }
        } catch (Exception ex) {
            logger.log("Exception handling batch seed request.");
            try {
                throw ex;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        PersonResponse personResponse = new PersonResponse();
        personResponse.setMessage("The person has been saved.");
        return personResponse;
    }

    private PutItemOutcome persistData(PersonRequest personRequest) throws ConditionalCheckFailedException, IOException {
        String cityCSV;
        cityCSV = ReadCSV(personRequest.getId());
        if (!cityCSV.equals("") && !cityCSV.equals(null)) personRequest.setCity(cityCSV);

        if (cityCSV.equals(null) || cityCSV.equals("")) {
            return this.dynamoDb.getTable(TABLE_NAME).putItem(new PutItemSpec().withItem(new Item()
                    .withNumber("id", personRequest.getId()).withString("firstName", personRequest.getFirstName())
                    .withString("lastName", personRequest.getLastName())));

        }
        return this.dynamoDb.getTable(TABLE_NAME).putItem(new PutItemSpec().withItem(new Item()
                .withNumber("id", personRequest.getId()).withString("firstName", personRequest.getFirstName())
                .withString("lastName", personRequest.getLastName()).withString("city", personRequest.getCity())));
    }

    public String ReadCSV(int id) throws IOException {

        AmazonS3 s3client = AmazonS3ClientBuilder
                .standard()
                .withRegion(Regions.EU_CENTRAL_1)
                .build();
        S3Object s3object = s3client.getObject(new GetObjectRequest(
                S3_NAME, S3_KEY));

        BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] cities = line.split(";");
            if (Integer.parseInt(cities[0]) == id) return cities[1];
        }
        return "";
    }

    private void initDynamoDbClient() {
        AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.EU_CENTRAL_1).build();
        this.dynamoDb = new DynamoDB(ddb);
    }
}