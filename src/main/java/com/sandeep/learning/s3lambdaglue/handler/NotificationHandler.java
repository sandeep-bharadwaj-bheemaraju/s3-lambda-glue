package com.sandeep.learning.s3lambdaglue.handler;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.StartJobRunRequest;
import com.amazonaws.services.glue.model.StartJobRunResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.HashMap;
import java.util.Map;


public class NotificationHandler implements RequestHandler<S3Event, String> {

	private AmazonDynamoDB amazonDynamoDB;

	private DynamoDB dynamoDB;

	private AmazonS3 amazonS3;

	private AWSGlue awsGlue;

	private Map<String, String> configMap;

	public NotificationHandler() {

		amazonDynamoDB = AmazonDynamoDBClientBuilder.standard().build();

		dynamoDB = new DynamoDB(amazonDynamoDB);

		amazonS3 = AmazonS3ClientBuilder.standard().build();

		awsGlue =  AWSGlueClientBuilder.standard().build();

		loadConfiguration();
	}


	@Override
	public String handleRequest(S3Event event, Context context) {

		context.getLogger().log("Event generated is [" +event.toJson()+ "]");

		String bucketName   = event.getRecords().get(0).getS3().getBucket().getName();
		String fileKey     = event.getRecords().get(0).getS3().getObject().getKey();
		StringBuilder inProcessFiles = new StringBuilder();
		String fileName;

		ObjectListing readyFileList = amazonS3.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(configMap.get("READY-DIR-PATH")));
		int readyFilesCount = readyFileList.getObjectSummaries().size()-1;

		context.getLogger().log("READY STATE FILES ARE - "+readyFilesCount);

		for (S3ObjectSummary summary : readyFileList.getObjectSummaries()) {
			context.getLogger().log("[" +summary.getKey()+ "]");
		}

		ObjectListing inProcessFileList = amazonS3.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(configMap.get("IN-PROCESS-DIR-PATH")));
		int inProcessFilesCount = inProcessFileList.getObjectSummaries().size()-1;

		context.getLogger().log("IN-PROCESS STATE FILES ARE - "+inProcessFilesCount);

		for (S3ObjectSummary summary : inProcessFileList.getObjectSummaries()) {
			context.getLogger().log("[" +summary.getKey()+ "]");
		}

		if(inProcessFilesCount == 0  && readyFilesCount > 0) {

			for (S3ObjectSummary summary : readyFileList.getObjectSummaries()) {

				fileName = summary.getKey().substring(fileKey.lastIndexOf("/") + 1);

				if(!fileName.contains(".csv"))
					continue;

				amazonS3.copyObject(bucketName, summary.getKey(), bucketName, configMap.get("IN-PROCESS-DIR-PATH") + fileName);

				amazonS3.deleteObject(bucketName, summary.getKey());

				context.getLogger().log("FILE ["+fileName+"] MOVED TO IN-PROCESS STATE");

				inProcessFiles.append(fileName).append(",");
			}

			inProcessFiles = inProcessFiles.deleteCharAt(inProcessFiles.length()-1);

			context.getLogger().log("Starting Glue job [" +configMap.get("JOB")+ "] for files [" +inProcessFiles.toString()+ "]");

			StartJobRunResult startJobRunResult = awsGlue.startJobRun(new StartJobRunRequest().withJobName(configMap.get("JOB")));

			context.getLogger().log("Glue Job Id is [" +startJobRunResult.getJobRunId()+ "]");

			Table jobsTable = dynamoDB.getTable("jobs");

			jobsTable.putItem(new Item().with("JOB_ID",startJobRunResult.getJobRunId()).with("FILES",inProcessFiles.toString()).with("STATE", "IN-PROCESS"));

			return startJobRunResult.getJobRunId();

		} if(inProcessFilesCount == 0 && readyFilesCount == 0) {

			context.getLogger().log("NO FILES TO PROCESS..");

			return "NO FILES TO PROCESS";

		} else {

			context.getLogger().log("JOB RUN IN-PROGRESS..");

			return "JOB RUN IN-PROGRESS";
		}
	}


	private void loadConfiguration() {
		configMap = new HashMap<>();

		Table configTable = dynamoDB.getTable("configuration");

		Item item = configTable.getItem("CONFIG_KEY", "JOB");
		configMap.put("JOB", item.getString("CONFIG_VALUE"));

		item = configTable.getItem("CONFIG_KEY", "CRAWLER");
		configMap.put("CRAWLER", item.getString("CONFIG_VALUE"));

		item = configTable.getItem("CONFIG_KEY", "READY-DIR-PATH");
		configMap.put("READY-DIR-PATH", item.getString("CONFIG_VALUE"));

		item = configTable.getItem("CONFIG_KEY", "IN-PROCESS-DIR-PATH");
		configMap.put("IN-PROCESS-DIR-PATH", item.getString("CONFIG_VALUE"));
	}

}
