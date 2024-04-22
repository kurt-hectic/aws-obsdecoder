import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import {
  aws_s3 as s3, aws_iam as iam, aws_ec2 as ec2,
  aws_ecs as ecs, aws_lambda as lambda,
  aws_kinesis as kinesis, aws_s3_notifications as s3_notify, 
  aws_sqs as sqs, aws_dynamodb as ddb, aws_events as events, aws_events_targets as targets, RemovalPolicy
} from 'aws-cdk-lib';

import { SqsEventSource, KinesisEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';

import * as destinations from '@aws-cdk/aws-kinesisfirehose-destinations-alpha';
import * as firehose from "@aws-cdk/aws-kinesisfirehose-alpha";

import { DatabaseClusterEngine, ServerlessCluster, Credentials, AuroraCapacityUnit, DatabaseSecret } from 'aws-cdk-lib/aws-rds'
import * as fs from 'fs'

// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class ObsdecoderStack extends cdk.Stack {

  vpc: ec2.Vpc;
  bucket: s3.Bucket;
  dbCluster: ServerlessCluster;
  dbCredentials: DatabaseSecret;
  obsStream: kinesis.Stream;

  metricPolicy: iam.Policy;

  secrets = JSON.parse(fs.readFileSync('env_secret.json', 'utf-8'));
  RETENTION: number = 24; // 24 hours, records only available 24h in cache   


  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    this.vpc = new ec2.Vpc(this, 'Vpc', {
      maxAzs: 2,
      natGateways : 1,
      subnetConfiguration: [{
        cidrMask: 24,
        name: 'public',
        subnetType: ec2.SubnetType.PUBLIC,
      }, {
        cidrMask: 24,
        name: 'compute',
        subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
      }, {
        cidrMask: 28,
        name: 'rds',
        subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
      }]
    })

    this.bucket = new s3.Bucket(this, "Kinesis2RDSBucket", {
      versioned: false, removalPolicy: cdk.RemovalPolicy.DESTROY, autoDeleteObjects: true,
      lifecycleRules: [
        { expiration: cdk.Duration.days(90) }
      ]
    });

    const policyStatement = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ["cloudwatch:PutMetricData"],
      resources: ['*'],
    });

    this.metricPolicy =  new iam.Policy(this, "publish-metric-policy", { 
      statements: [ policyStatement ]
    })  

    this.obsStream = this.setupSurfaceObsPipeline();
    this.setupBridge(this.obsStream);
    

    this.setupDBInfrastructure(id);
    this.setupDBImport(id);
    this.setupExport()

  }


  setupBridgeTask( cluster: ecs.Cluster, image: ecs.ContainerImage, policyStatement: iam.PolicyStatement, stream: kinesis.Stream, environment: { [key: string]: string } , prefix : string ): void {


    const taskDefinition = new ecs.FargateTaskDefinition(this, "BridgeTask_"+prefix, { family: "BridgeTask_"+prefix, memoryLimitMiB: 512, cpu: 256  });

  
    const container = taskDefinition.addContainer("BridgeApp_"+prefix, {
      image: image,
      environment: environment,
      logging: new ecs.AwsLogDriver({ streamPrefix: "BrideLog_"+prefix, mode: ecs.AwsLogDriverMode.NON_BLOCKING })
    });

    const service = new ecs.FargateService(this, "BridgeService_"+prefix, {
      cluster: cluster,
      taskDefinition: taskDefinition, desiredCount: 1 
    });

    taskDefinition.addToTaskRolePolicy(policyStatement);
    taskDefinition.addToExecutionRolePolicy(policyStatement);

    stream.grantWrite(taskDefinition.taskRole);

  }


  setupBridge(stream : kinesis.Stream): void {


    const cluster = new ecs.Cluster(this, "BridgeEcsCluster", { vpc: this.vpc });

    const policyStatement = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ["cloudwatch:PutMetricData"],
      resources: ['*'],
    });
    
    const image = ecs.ContainerImage.fromAsset("./docker/wis2bridge");

    const reporting_threshold = "500" // send bridge statistics every x records
    const batch_size = "50" // group x records together before sending to kinesis stream

    const my_environment = {
      "WIS_USERNAME": this.secrets["WIS_MF_USERNAME"], "WIS_PASSWORD": this.secrets["WIS_MF_PASSWORD"], 
      "TOPICS": "cache/a/wis2/+/+/data/core/weather/surface-based-observations/synop,cache/a/wis2/+/data/core/weather/surface-based-observations/synop",
      "CLIENT_ID": "wis2bridge_", 
      "WIS_BROKER_HOST": "globalbroker.meteo.fr", "WIS_BROKER_PORT": "8883","VALIDATE_SSL" : "True",
       "LOG_LEVEL": "INFO",
       "STREAM_NAME" : this.obsStream.streamName,
       "REPORTING_THRESHOLD" : reporting_threshold, "BATCH_SIZE" : batch_size
    };

    this.setupBridgeTask(cluster, image, policyStatement, stream, my_environment, "MF");


    const my_environment_cma = {
      "WIS_USERNAME": this.secrets["WIS_USERNAME"], "WIS_PASSWORD": this.secrets["WIS_PASSWORD"], 
      "TOPICS": "cache/a/wis2/+/+/data/core/weather/surface-based-observations/synop,cache/a/wis2/+/data/core/weather/surface-based-observations/synop",
      "CLIENT_ID": "wis2bridge_cma_", 
      "WIS_BROKER_HOST": "gb.wis.cma.cn", "WIS_BROKER_PORT": "1883", 
       "LOG_LEVEL": "INFO",
       "STREAM_NAME" : this.obsStream.streamName,
       "REPORTING_THRESHOLD" : reporting_threshold, "BATCH_SIZE" : batch_size
    };

    this.setupBridgeTask(cluster, image, policyStatement, stream, my_environment_cma, "CMA");
    
    const my_environment_noaa = {
      "WIS_USERNAME": this.secrets["WIS_USERNAME"], "WIS_PASSWORD": this.secrets["WIS_PASSWORD"], 
      "TOPICS": "cache/a/wis2/+/+/data/core/weather/surface-based-observations/synop,cache/a/wis2/+/data/core/weather/surface-based-observations/synop",
      "CLIENT_ID": "wis2bridge_noaa_", 
      "WIS_BROKER_HOST": "wis2globalbroker.nws.noaa.gov", "WIS_BROKER_PORT": "1883",
       "LOG_LEVEL": "INFO",
       "STREAM_NAME" : this.obsStream.streamName,
       "REPORTING_THRESHOLD" : reporting_threshold, "BATCH_SIZE" : batch_size
    };

    this.setupBridgeTask(cluster, image, policyStatement, stream, my_environment_noaa, "NOAA");

    const my_environment_inmet = {
      "WIS_USERNAME": this.secrets["WIS_USERNAME"], "WIS_PASSWORD": this.secrets["WIS_PASSWORD"], 
      "TOPICS": "cache/a/wis2/+/+/data/core/weather/surface-based-observations/synop,cache/a/wis2/+/data/core/weather/surface-based-observations/synop",
      "CLIENT_ID": "wis2bridge_inmet_", 
      "WIS_BROKER_HOST": "globalbroker.inmet.gov.br", "WIS_BROKER_PORT": "8883", "VALIDATE_SSL" : "False",
       "LOG_LEVEL": "INFO",
       "STREAM_NAME" : this.obsStream.streamName,
       "REPORTING_THRESHOLD" : reporting_threshold, "BATCH_SIZE" : batch_size
    };

    this.setupBridgeTask(cluster, image, policyStatement, stream, my_environment_inmet, "INMET");

  }

  setupSurfaceObsPipeline(): kinesis.Stream {

    const dataIDtable = new ddb.Table(this, 'dataIdTable', {
      partitionKey: { name: 'dataid', type: ddb.AttributeType.STRING },
      //sortKey: { name: 'date', type: ddb.AttributeType.NUMBER },
      billingMode: ddb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: RemovalPolicy.DESTROY,
      timeToLiveAttribute : "expirationdate"
    });


   
    const obsStream = new kinesis.Stream(this, "SurfaceObsInputStream", { 
      retentionPeriod: cdk.Duration.hours(this.RETENTION), 
      streamMode: cdk.aws_kinesis.StreamMode.PROVISIONED,
      shardCount: 2
    })

    
    const firehoseStream = new firehose.DeliveryStream(this, 'SurfaceObsDeliveryStream', {
      destinations: [new destinations.S3Bucket(this.bucket, {
        dataOutputPrefix: "to-be-processed/surface-observations/",
        bufferingInterval: cdk.Duration.seconds(120),
        //bufferingSize: cdk.Size.mebibytes(1)  
      })]
      ,
    });

    const lambdaFunction = new lambda.DockerImageFunction(this, "SurfaceObsLambda", {
      code: lambda.DockerImageCode.fromImageAsset(
        "./docker/", {
        file: "Dockerfile-lambda_surface-obs",
        exclude: ["./wis2bridge","./lambda_s3export"] // do not re-deploy for changes in these directories
      }),
      environment: { 
        "LAMBDA_LOG_LEVEL": "INFO", 
        "FIREHOSE_NAME": firehoseStream.deliveryStreamName , 
        "DDB_NAME" : dataIDtable.tableName,
        "DISABLE_FILTER" : "False" 
      },
      timeout: cdk.Duration.minutes(10),
      vpc: this.vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      memorySize: 256
    });

    lambdaFunction.role?.attachInlinePolicy(this.metricPolicy);
    firehoseStream.grantPutRecords(lambdaFunction);
    dataIDtable.grantReadWriteData(lambdaFunction);


    lambdaFunction.addEventSource(new KinesisEventSource(obsStream, {
      batchSize: 50, // default
      maxBatchingWindow: cdk.Duration.seconds(60),
      startingPosition: lambda.StartingPosition.TRIM_HORIZON,
      reportBatchItemFailures: false,
      bisectBatchOnError: true,
      parallelizationFactor: 10
    }));


    return obsStream;



  }

  
  setupDBInfrastructure(id: string): void {

    const instanceIdentifier = 'mysql-01'
    const credsSecretName = `/${id}/rds/creds/${instanceIdentifier}`.toLowerCase()
    this.dbCredentials = new DatabaseSecret(this, 'MysqlRdsCredentials', {
      secretName: credsSecretName,
      username: 'admin'
    });

    const auroraSg = new ec2.SecurityGroup(this, "SecurityGroup", {
      vpc: this.vpc,
      description: "Allow ssh access to aurora cluster",
      allowAllOutbound: true
    })

    auroraSg.addIngressRule(
      ec2.Peer.ipv4(this.vpc.vpcCidrBlock),
      ec2.Port.tcp(3306)
    )


    this.dbCluster = new ServerlessCluster(this, "AuroraCluster", {
      engine: DatabaseClusterEngine.AURORA_MYSQL,
      vpc: this.vpc,
      enableDataApi: true,
      credentials: Credentials.fromSecret(this.dbCredentials),
      defaultDatabaseName: "main",
      securityGroups: [auroraSg],
      scaling: {
        autoPause: cdk.Duration.minutes(10), // default is to pause after 5 minutes of idle time
        minCapacity: AuroraCapacityUnit.ACU_1, // default is 2 Aurora capacity units (ACUs)
        maxCapacity: AuroraCapacityUnit.ACU_16, // default is 16 Aurora capacity units (ACUs)
      }
    })

    // acces the DB via mini EC2 instance
    const bastionSG = new ec2.SecurityGroup(this, 'BastionSg', {
      vpc: this.vpc,
      allowAllOutbound: true,
    });

    bastionSG.addIngressRule(
      ec2.Peer.anyIpv4(),
      ec2.Port.tcp(22),
      'allow SSH access from anywhere',
    );

    const bastionEc2Instance = new ec2.Instance(this, 'BastionEc2Instance', {
      vpc: this.vpc,
      vpcSubnets: {
        subnetType: ec2.SubnetType.PUBLIC,
      },
      securityGroup: bastionSG,
      instanceType: ec2.InstanceType.of(
        ec2.InstanceClass.BURSTABLE2,
        ec2.InstanceSize.MICRO,
      ),
      machineImage: new ec2.AmazonLinuxImage({
        generation: ec2.AmazonLinuxGeneration.AMAZON_LINUX_2,
      }),
      keyName: 'wis2monitoring-key',
    });

    new cdk.CfnOutput(this, "Bastion IP address", {
      value: bastionEc2Instance.instancePublicIp
    });

    this.dbCluster.connections.allowFrom(bastionEc2Instance, ec2.Port.tcp(3306))

    // cleanup DB

    const cleanupLambda = new lambda.Function(this, 'cleanupLambda', {
      code: lambda.Code.fromAsset('lambda/cleanup'),
      handler: 'cleanup.handler',
      runtime: lambda.Runtime.PYTHON_3_9,
      timeout: cdk.Duration.minutes(10),
      environment: {
        "BUCKET": this.bucket.bucketArn,
        "CLUSTER_ARN": this.dbCluster.clusterArn,
        "SECRET_ARN": this.dbCredentials.secretArn,
        "DB_NAME": "main",
        "NR_DAYS_KEEP": "180",
        "LAMBDA_LOG_LEVEL": "DEBUG"
      }

    });

    // allow cleanup function access to credentials and database API
    this.dbCluster.grantDataApiAccess(cleanupLambda);


    const event = new events.Rule(this, 'cleanupLambdaRule', {
      description: "cleanup with DB periodically",
      targets: [new targets.LambdaFunction(cleanupLambda)],
      schedule: events.Schedule.rate(cdk.Duration.days(1)),
    }
    );

  }

  setupDBImport(id: string): void {


    // setup Queues

    const surfaceObsSource = this.setupImport(id+"_surfaceobservations", "to-be-processed/surface-observations/")
    
    const fn = new lambda.Function(this, "S3importFunction", {
      code: lambda.Code.fromAsset("lambda/s3tords"),
      handler: 'app.lambda_handler',
      runtime: lambda.Runtime.PYTHON_3_9,
      environment: {
        "BUCKET": this.bucket.bucketArn,
        "CLUSTER_ARN": this.dbCluster.clusterArn,
        "SECRET_ARN": this.dbCredentials.secretArn,
        "DB_NAME": "main",
        "LAMBDA_LOG_LEVEL": "INFO",
        "PROCESSED_PREFIX": "processed",
        "DB_BATCH_SIZE": "1000",
        "SURFACEOBS_SOURCE_ARN": surfaceObsSource.queue.queueArn,
      },
      timeout: cdk.Duration.minutes(10),
      vpc: this.vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      architecture: lambda.Architecture.X86_64
    });

    fn.role?.attachInlinePolicy(this.metricPolicy);

    this.bucket.grantReadWrite(fn);
    this.dbCluster.grantDataApiAccess(fn);
    //this.dbCredentials.grantRead(fn); // included in call above

  fn.addEventSource(surfaceObsSource);
    
    // surface-obs

  }

  setupImport(name: string, prefix: string): SqsEventSource {

    const deadLetterQueue = new sqs.Queue(this, "DLDqueue_" + name, {
      queueName: "dlq_" + name,
      deliveryDelay: cdk.Duration.millis(0),
      retentionPeriod: cdk.Duration.days(14),
    });

    const queue = new sqs.Queue(this, "S3ImportQueue_" + name, {
      queueName: "S3ImportQueue_" + name,
      visibilityTimeout: cdk.Duration.minutes(6 * 3),
      deadLetterQueue: {
        maxReceiveCount: 2,
        queue: deadLetterQueue
      }
    });

    this.bucket.addEventNotification(s3.EventType.OBJECT_CREATED, new s3_notify.SqsDestination(queue), {
      prefix: prefix
    });

    const eventSource = new SqsEventSource(queue, {
      reportBatchItemFailures: true,
      batchSize: 5, // maximum number of files processed by one lambda invocation
      maxBatchingWindow: cdk.Duration.seconds(10),
      maxConcurrency: 3 // maximum number of parallel lambda invocations
    });


    return eventSource

  }

  setupExport(): void {

    
    const exportbucket = new s3.Bucket(this, "Export", {
      versioned: false, removalPolicy: cdk.RemovalPolicy.DESTROY, autoDeleteObjects: true,
      publicReadAccess: true,
      blockPublicAccess: {
          blockPublicAcls: false,
          blockPublicPolicy: false,
          ignorePublicAcls: false,
          restrictPublicBuckets: false,
      },
      lifecycleRules: [
        { expiration: cdk.Duration.days(90) }
      ]
    });
    
    const lambdaFunction = new lambda.DockerImageFunction(this, "exportLambda", {
      code: lambda.DockerImageCode.fromImageAsset(
        "./docker/", {
        file: "Dockerfile-lambda_s3export",
        exclude: ["./wis2bridge", "./lambda_surface-obs","./wis2mon-lib"] // do not re-deploy for changes in these directories
      }),
      environment: { 
        "LAMBDA_LOG_LEVEL": "INFO", 
        "SECRET_NAME": this.dbCredentials.secretName,
        "BUCKET_NAME" : exportbucket.bucketName,
        "DB_OVERRIDE" : "False",
        "COMPRESS_FILE" : "False"
      },
      timeout: cdk.Duration.minutes(10),
      vpc: this.vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      memorySize: 128*8,
    });

    this.dbCredentials.grantRead(lambdaFunction)
    this.dbCluster.connections.allowFrom(lambdaFunction, ec2.Port.tcp(3306))
 

    exportbucket.grantReadWrite(lambdaFunction);
    exportbucket.grantPublicAccess();

    lambdaFunction.role?.attachInlinePolicy(this.metricPolicy);

    //exportbucket.addToResourcePolicy( new iam.PolicyStatement( { actions: ["s3:ListObjects"] , resources : [exportbucket.arnForObjects('*')] , principals: [ new iam.AnyPrincipal()]  } ) );

    // exportbucket.addToResourcePolicy(
    //   new iam.PolicyStatement({
    //     effect: iam.Effect.ALLOW,
    //     principals: [new iam.AnyPrincipal() ],
    //     actions: ['s3:ListBucket','s3:GetObject'],
    //     resources: [`${exportbucket.bucketArn}/*`],
    //   }),
    // );

    const event = new events.Rule(this,'my-lambda-rule',
      {
        description: "Produce 6h file 55 minutes after window end to give enough time to process all records",
        targets: [
          new targets.LambdaFunction(lambdaFunction),
        ],
        schedule:  events.Schedule.expression('cron(55 3,9,15,21 ? * * *)') 
      }
    );

  }


}
