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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1758659801822 = glueContext.create_dynamic_frame.from_catalog(database="steadi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1758659801822")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1758659803124 = glueContext.create_dynamic_frame.from_catalog(database="steadi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1758659803124")

# Script generated for node SQL Query
SqlQuery2691 = '''
select acc_trusted.user, stt_trusted.serialnumber, acc_trusted.x, acc_trusted.y, acc_trusted.z, stt_trusted.distancefromobject,
stt_trusted.sensorreadingtime 
from acc_trusted 
join stt_trusted
on acc_trusted.timestamp = stt_trusted.sensorreadingtime
'''
SQLQuery_node1758661642476 = sparkSqlQuery(glueContext, query = SqlQuery2691, mapping = {"acc_trusted":accelerometer_trusted_node1758659801822, "stt_trusted":step_trainer_trusted_node1758659803124}, transformation_ctx = "SQLQuery_node1758661642476")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758661642476, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758648355119", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1758659846511 = glueContext.getSink(path="s3://isi123buck/machine_learning/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1758659846511")
machine_learning_curated_node1758659846511.setCatalogInfo(catalogDatabase="steadi",catalogTableName="machine_learning_curated")
machine_learning_curated_node1758659846511.setFormat("json")
machine_learning_curated_node1758659846511.writeFrame(SQLQuery_node1758661642476)
job.commit()