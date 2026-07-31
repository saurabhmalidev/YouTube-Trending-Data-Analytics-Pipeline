


#========================================================================
Storage :
=========
	S3 : Storage Buckets
		Bronze Layer : yt-data-pipeline-bronze-dev001
		Silver Layer : yt-data-pipeline-silver-dev001
		Gold Layer   : yt-data-pipeline-gold-dev001
		Script 		 : yt-data-pipeline-scripts-dev001
		Athena Query Storage : yt-data-pipeline-glue-athena-result-bucket
		
		Glue Logging : aws-glue-assets-962988650758-ap-south-1
#========================================================================
IAM User :	
==========

	Name : yt-pipeline-admin
	Access Level: Administrative Access.
	
#========================================================================
Roles :
=======
	1. yt-data-pipeline-glue-role-dev
			- AmazonS3FullAccess
			- AWSGlueServiceRole  
			- Inline Policy for the minimum access
					1. Glue
					2. SNS
					3. Athena
					4. S3
			
	2. yt-data-pipeline-lambda-role-dev
			- AWSLambdaBasicExecution
			- Inline Policy for -	
					1. SNS
					2. Athena
					3. Glue
					4. S3
					
	3. yt-data-pipeline-stepFunction-role-dev
			- Inline Policy - (yt-data-pipeline-stepFunction-role-dev-policy)
					1. "Action": "lambda:InvokeFunction"
					2. "Action": ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"],"Resource": "*"
					3. "Effect": "Allow", "Action": "sns:Publish", "Resource": "arn:aws:sns:ap-south-1:962988650758:yt-data-pipeline-*"
	
	
Data Sources :
	
#========================================================================
GLUE
====
	1. Glue ETL Jobs :
	
		1.1 # Name : yt-data-pipeline-bronze_to_silver_statistics-dev
		
			# Config
				────────────────────────────────────────────
				Glue Job: Bronze → Silver (Statistics Data)
				
				Reads raw CSV/JSON statistics from the Bronze layer,
				applies schema enforcement, data cleansing, deduplication,
				and writes clean Parquet to the Silver layer.
				
				Improvements over original pyspark_code.py:
				- Data quality checks with row-level flagging
				- Deduplication (same video appearing in multiple ingestions)
				- Date parsing and standardization
				- Handles both Kaggle CSV format and live API JSON format
				- Partitioned by region AND date for better query performance
				- Bookmarking for incremental processing
				- Proper logging
				
				Job Parameters:
					--JOB_NAME                   — Glue job name (auto-set)
					--bronze_database            — yt-pipeline-bronze_db
					--bronze_table               — raw_statistics
					--silver_bucket              — yt-data-pipeline-silver-dev001
					--silver_database            — yt-pipeline-silver_db
					--silver_table               — clean_statistics
				────────────────────────────────────────────

		
		
		1.2 # Glue Job Name : yt-data-pipeline-silver_to_gold-analytics-dev
				
			# Config :
				─────────────────────────────────────────────────
				Glue Job: Silver → Gold (Analytics Aggregations)
				
				Reads cleansed statistics and reference data from Silver,
				joins them, and produces business-level aggregations in the Gold layer.
				
				Gold layer tables are optimized for analytics queries in Athena/QuickSight.
				
				Gold tables produced:
				1. trending_analytics   — Daily trending summaries per region
				2. channel_analytics    — Channel performance metrics
				3. category_analytics   — Category-level trends over time
				
				Job Parameters:
					--JOB_NAME              — 
					--silver_database       — yt-pipeline-silver_db
					--gold_bucket           — yt-data-pipeline-gold-dev001
					--gold_database         — yt-pipeline-gold_db
				─────────────────────────────────────────────────
		
		
		1.3 Common Config :
				1. IAM Role : yt-data-pipeline-glue-role-dev
				2. Type : Spark
				3. Glue Version : (Glue5.1 - Spark 3.5, Scala 2, Python 3)
				4. Worker Type : G 1X (4vCPU, 16GB RAM)
				5. Requested number of workers : 2
	
	2. Glue Data Catalog  Databases and Table:
			Databases:
				1. yt-pipeline-bronze_db
						1. raw_statistics (table)
						2. raw_statistics_reference_data (table)
				2. yt-pipeline-silver_db
						1. clean_statistics (table)
						2. clean_reference_data (table)
				3. yt-pipeline-gold_db
						1. category_analytics
						2. channel_analytics
						3. trending_analytics


		
#========================================================================
SNS  Simple Notification Service :
	Topic:
		Name - yt-data-pipeline-sns-alerts-dev
		ARN : arn:aws:sns:ap-south-1:962988650758:yt-data-pipeline-sns-alerts-dev
			  arn:aws:sns:ap-south-1:xxxxxxxxxxxx:yt-data-pipeline-sns-alerts-dev
		Subscription : email
	
	
#========================================================================
Lambda Function 
===============
	1. 	Name : yt-data-pipeline-yt-lambda-ingestion-dev (arn:aws:lambda:ap-south-1:962988650758:function:yt-data-pipeline-yt-lambda-ingestion-dev)
		
		File Name : lambda_function.py
			
		Parameters : 	
			1. S3_BUCKET_BRONZE - yt-data-pipeline-bronze-dev001
			2. SNS_ALERT_TOPIC_ARN - arn:aws:sns:ap-south-1:962988650758:yt-data-pipeline-sns-alerts-dev
			3. YOUTUBE_API_KEY - AIzaSyD532ieN56V7il7l-FL-fxWYr9oy0jmK9g
			4. YOUTUBE_REGIONS - US,GB,CA,IN
		
	
	2. Name : yt-data-pipeline-yt-lambda-json-to-parquet-dev (arn:aws:lambda:ap-south-1:962988650758:function:yt-data-pipeline-yt-lambda-json-to-parquet-dev)
	
		File Name : lambda_function.py
		
		Parameters :
			1. GLUE_DB_SILVER - yt-pipeline-silver_db
			2. S3_BUCKET_SILVER - yt-data-pipeline-silver-dev001
			3. SNS_ALERT_TOPIC_ARN - arn:aws:sns:ap-south-1:962988650758:yt-data-pipeline-sns-alerts-dev
	
	3. Name : yt-data-pipeline-data-quality-dev 
	(arn:aws:lambda:ap-south-1:962988650758:function:yt-data-pipeline-data-quality-dev)
	
		File Name : lambda_function.py
		
		Parameters :
			1. SNS_ALERT_TOPIC_ARN - arn:aws:sns:ap-south-1:962988650758:yt-data-pipeline-sns-alerts-dev
			
	4. Common Config for all Lambda Function :
	
		a. General Config :- Memory : 512 MB | Ephemeral storage : 1024 MB | Timeout : 5 Min 03 Sec
		b. Runtime Setting :
				- Runtime : Python 3.12
				- Handler : lambda_function.lambda_handler
				- Architecture : x86_64
		c. Layer : 
				- Name : AWSSDKPandas-Python312
				- Compatible Runtime : python3.12
		d. Monitoring :
				- CloudWatch log group
				- Log Format : text
		
#========================================================================
Step Function :

#========================================================================
Athena 
	- Will fetch data vie the Glue Data Catalog.
