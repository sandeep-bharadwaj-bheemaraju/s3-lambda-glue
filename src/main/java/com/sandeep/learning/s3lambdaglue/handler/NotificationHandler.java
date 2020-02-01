package com.sandeep.learning.s3lambdaglue.handler;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.StartCrawlerRequest;
import com.amazonaws.services.glue.model.StartJobRunRequest;
import com.amazonaws.services.glue.model.StartJobRunResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;

public class NotificationHandler implements RequestHandler<S3Event, String> {

	private AmazonDynamoDB amazonDynamoDB;

	private DynamoDB dynamoDB;

	private AWSGlue awsGlue;

	public NotificationHandler() {

		amazonDynamoDB = AmazonDynamoDBClientBuilder.standard().build();

		dynamoDB = new DynamoDB(amazonDynamoDB);

		awsGlue =  AWSGlueClientBuilder.standard().build();
	}


	@Override
	public String handleRequest(S3Event event, Context context) {

		// Get the object from the event and fetch bucketName and fileName
		String bucketName   = event.getRecords().get(0).getS3().getBucket().getName();
		String fileName     = event.getRecords().get(0).getS3().getObject().getKey();

		Table table = dynamoDB.getTable("configuration");

		Item item = table.getItem("KEY", "JOB");
		String jobName = item.getString("VALUE");

		item = table.getItem("KEY", "CRAWLER");
		String crawlerName = item.getString("VALUE");

		context.getLogger().log("Crawling [" +bucketName+ "] for file [" +fileName+ "] using CRAWLER [" +crawlerName+ "]");

		awsGlue.startCrawler(new StartCrawlerRequest().withName(crawlerName));

		context.getLogger().log("Starting Glue job [" +jobName+ "]");

		StartJobRunResult startJobRunResult = awsGlue.startJobRun(new StartJobRunRequest().withJobName(jobName));

		context.getLogger().log("Glue Job Id is [" +startJobRunResult.toString()+ "]");

		return startJobRunResult.toString();
	}

}
