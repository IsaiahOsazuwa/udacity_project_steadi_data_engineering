import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer curated
customercurated_node1758650644223 = glueContext.create_dynamic_frame.from_catalog(database="steadi", table_name="customer_curated", transformation_ctx="customercurated_node1758650644223")

# Script generated for node step_trainer_landing
step_trainer_landing_node1758648373687 = glueContext.create_dynamic_frame.from_catalog(database="steadi", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1758648373687")

# Script generated for node match customer curated
SqlQuery2798 = '''
select mt.sensorreadingtime, mt.serialnumber, mt.distancefromobject
from step_train as mt
join customer_curated as cc
on mt.serialnumber = cc.serialnumber
'''
matchcustomercurated_node1758650878373 = sparkSqlQuery(glueContext, query = SqlQuery2798, mapping = {"customer_curated":customercurated_node1758650644223, "step_train":step_trainer_landing_node1758648373687}, transformation_ctx = "matchcustomercurated_node1758650878373")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=matchcustomercurated_node1758650878373, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758648355119", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758651384652 = glueContext.getSink(path="s3://isi123buck/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1758651384652")
AmazonS3_node1758651384652.setCatalogInfo(catalogDatabase="steadi",catalogTableName="step_trainer_trusted")
AmazonS3_node1758651384652.setFormat("json")
AmazonS3_node1758651384652.writeFrame(matchcustomercurated_node1758650878373)
job.commit()