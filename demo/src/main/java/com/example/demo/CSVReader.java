package com.example.demo;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class CSVReader {

    private String S3_NAME = "my-s3-test-task";
    private String S3_KEY = "s3.csv";

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
}
