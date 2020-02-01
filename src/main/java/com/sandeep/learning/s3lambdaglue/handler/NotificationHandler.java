package com.sandeep.learning.s3lambdaglue.handler;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;

public class NotificationHandler implements RequestHandler<S3Event, String> {

	private AmazonDynamoDB amazonDynamoDB;

	private DynamoDB dynamoDB;


	public NotificationHandler() {

		amazonDynamoDB = AmazonDynamoDBClientBuilder.standard().build();

		dynamoDB = new DynamoDB(amazonDynamoDB);
	}


	@Override
	public String handleRequest(S3Event event, Context context) {

		Table table = dynamoDB.getTable("configuration");
		Item item = table.getItem("KEY", "job.name");
		String jobName = item.getString("VALUE");

		context.getLogger().log("Glue job to be invoked is : " +jobName);

		// Get the object from the event and fetch bucketName and fileName
		String bucketName   = event.getRecords().get(0).getS3().getBucket().getName();
		String fileName     = event.getRecords().get(0).getS3().getObject().getKey();

		context.getLogger().log("Bucket name is : " +bucketName);
		context.getLogger().log("File name is   : " +fileName);

		return bucketName;
	}

}
