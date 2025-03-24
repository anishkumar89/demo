import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as cr from 'aws-cdk-lib/custom-resources';

const s3SatsPipelineBucket = new s3.Bucket(scope, "SATSPipelineBucket", {
  bucketName: resourceNamePrefix + "pipeline-data-stage",
  removalPolicy: cdk.RemovalPolicy.DESTROY,
  autoDeleteObjects: true,
  enforceSSL: true,
  encryption: s3.BucketEncryption.KMS,
  encryptionKey: s3KmsKey,  // Ensure this KMS key is defined
  serverAccessLogsBucket: s3SatsPipelineLoggingBucket, // Ensure logging bucket exists
  serverAccessLogsPrefix: 'access-logs/',
  blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
  lifecycleRules: [
    {
      expiration: cdk.Duration.days(31),
    },
  ],
});

// IAM Role for Custom Resource to Create the "WIP" Folder
const s3CustomResourceRole = new iam.Role(scope, "S3CustomResourceRole", {
  assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
  managedPolicies: [
    iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonS3FullAccess"), // Adjust permissions as needed
  ],
});

// Custom Resource to Create "WIP/" Folder
new cr.AwsCustomResource(scope, "CreateWIPFolder", {
  onCreate: {
    service: "S3",
    action: "putObject",
    parameters: {
      Bucket: s3SatsPipelineBucket.bucketName,
      Key: "WIP/", // This creates a folder-like prefix
    },
    physicalResourceId: cr.PhysicalResourceId.of(s3SatsPipelineBucket.bucketName + "/WIP"),
  },
  policy: cr.AwsCustomResourcePolicy.fromSdkCalls({ resources: [s3SatsPipelineBucket.arnForObjects("*")] }),
  role: s3CustomResourceRole,
});
