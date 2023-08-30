package amazon.emr.pig;

import java.io.File;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.ActionOnFailure;
import software.amazon.awssdk.services.emr.model.AddJobFlowStepsRequest;
import software.amazon.awssdk.services.emr.model.AddJobFlowStepsResponse;
import software.amazon.awssdk.services.emr.model.Application;
import software.amazon.awssdk.services.emr.model.DescribeStepRequest;
import software.amazon.awssdk.services.emr.model.DescribeStepResponse;
import software.amazon.awssdk.services.emr.model.HadoopJarStepConfig;
import software.amazon.awssdk.services.emr.model.JobFlowInstancesConfig;
import software.amazon.awssdk.services.emr.model.PlacementType;
import software.amazon.awssdk.services.emr.model.RunJobFlowRequest;
import software.amazon.awssdk.services.emr.model.RunJobFlowResponse;
import software.amazon.awssdk.services.emr.model.StepConfig;
import software.amazon.awssdk.services.emr.model.StepState;
import software.amazon.awssdk.services.emr.model.TerminateJobFlowsRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class JobManager {

	static S3Client s3client = S3Client.builder().region(Region.US_EAST_1)
			.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(null, null))).build();

	static EmrClient emrClient = EmrClient.builder().region(Region.US_EAST_1)
			.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(null, null))).build();

	public static void main(String[] args) throws InterruptedException {
		createBucketAndPutNecessaryObjects();
		createClusterAndRunPigJob();
	}

	private static void createBucketAndPutNecessaryObjects() {
		try {
			s3client.createBucket(CreateBucketRequest.builder().bucket("emr-pig-bucket-1").build());
		} catch (BucketAlreadyExistsException e) {
			e.printStackTrace();
		}
		s3client.putObject(PutObjectRequest.builder().bucket("emr-pig-bucket-1").key("names.txt").build(),
				RequestBody.fromFile(new File("names.txt")));
		s3client.putObject(PutObjectRequest.builder().bucket("emr-pig-bucket-1").key("names.pig").build(),
				RequestBody.fromFile(new File("names.pig")));
	}

	private static void createClusterAndRunPigJob() throws InterruptedException {
		Application hadoop = Application.builder().name("Hadoop").build();
		Application pig = Application.builder().name("Pig").build();

		StepConfig hiveJobStepConfig = StepConfig.builder().name("Simple Pig Job")
				.actionOnFailure(ActionOnFailure.TERMINATE_CLUSTER)
				.hadoopJarStep(HadoopJarStepConfig.builder().jar("command-runner.jar")
						.args("pig-script", "--run-pig-script", "--args", "-f", "s3://emr-pig-bucket-1/names.pig")
						.build())
				.build();

		RunJobFlowRequest request = RunJobFlowRequest.builder().name("My EMR Cluster").releaseLabel("emr-6.12.0")
				.applications(hadoop, pig).logUri("s3://emr-pig-bucket-1/logs").serviceRole("EMR_DefaultRole")
				.jobFlowRole("EMR_EC2_DefaultRole")
				.instances(JobFlowInstancesConfig.builder().ec2KeyName("kd1kp").instanceCount(1)
						.placement(PlacementType.builder().availabilityZones("us-east-1a").build())
						.keepJobFlowAliveWhenNoSteps(true).masterInstanceType("m4.large").build())
				.build();

		RunJobFlowResponse runJobFlowResponse = emrClient.runJobFlow(request);

		AddJobFlowStepsResponse addJobFlowStepsResponse = emrClient.addJobFlowSteps(AddJobFlowStepsRequest.builder()
				.jobFlowId(runJobFlowResponse.jobFlowId()).steps(hiveJobStepConfig).build());

		boolean stepsCompleted = false;
		while (!stepsCompleted) {
			DescribeStepResponse describeStepResponse = emrClient
					.describeStep(DescribeStepRequest.builder().clusterId(runJobFlowResponse.jobFlowId())
							.stepId(addJobFlowStepsResponse.stepIds().get(0)).build());
			if (describeStepResponse.step().status().state().equals(StepState.COMPLETED)
					|| describeStepResponse.step().status().state().equals(StepState.CANCELLED)
					|| describeStepResponse.step().status().state().equals(StepState.FAILED)) {
				TerminateJobFlowsRequest terminateJobFlowsRequest = TerminateJobFlowsRequest.builder()
						.jobFlowIds(runJobFlowResponse.jobFlowId()).build();
				emrClient.terminateJobFlows(terminateJobFlowsRequest);
			}
			Thread.sleep(5000L);
		}

	}

}
