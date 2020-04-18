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

public class SavePersonHandler implements RequestHandler<PersonRequest, PersonResponse> {

    private DynamoDB dynamoDb;

    private String TABLE_NAME = "persons";

    public PersonResponse handleRequest(PersonRequest personRequest, Context context) {
        LambdaLogger logger = context.getLogger();


        logger.log("----------> Get new person with name " + personRequest.getFirstName());

        this.initDynamoDbClient();

        persistData(personRequest);

        PersonResponse personResponse = new PersonResponse();
        personResponse.setMessage("The person has been saved.");
        return personResponse;
    }

    private PutItemOutcome persistData(PersonRequest personRequest) throws ConditionalCheckFailedException {
        String cityCSV;
        cityCSV = ReadCSV(personRequest.getId());
        if (!cityCSV.equals("") && !cityCSV.equals(null))personRequest.setCity(cityCSV);

        if (  cityCSV.equals(null) || cityCSV.equals("")){
            return this.dynamoDb.getTable(TABLE_NAME).putItem(new PutItemSpec().withItem(new Item()
                    .withNumber("id", personRequest.getId()).withString("firstName", personRequest.getFirstName())
                    .withString("lastName", personRequest.getLastName())));

        }
        return this.dynamoDb.getTable(TABLE_NAME).putItem(new PutItemSpec().withItem(new Item()
                .withNumber("id", personRequest.getId()).withString("firstName", personRequest.getFirstName())
                .withString("lastName", personRequest.getLastName()).withString("city", personRequest.getCity())));
    }
    public String ReadCSV(int id) {
        return "Paris";
    }
    private void initDynamoDbClient() {

        AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.EU_CENTRAL_1).build();
        this.dynamoDb = new DynamoDB(ddb);
    }
}