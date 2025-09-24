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

# Script generated for node customer_trusted
customer_trusted_node1758549004647 = glueContext.create_dynamic_frame.from_catalog(database="steadi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1758549004647")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1758549007732 = glueContext.create_dynamic_frame.from_catalog(database="steadi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1758549007732")

# Script generated for node Join
Join_node1758549120300 = Join.apply(frame1=customer_trusted_node1758549004647, frame2=accelerometer_trusted_node1758549007732, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1758549120300")

# Script generated for node filter
SqlQuery3351 = '''
select distinct customername, email, phone, birthday, serialnumber,
registrationdate, lastupdatedate, sharewithresearchasofdate, 
sharewithpublicasofdate, sharewithfriendsasofdate
from myDataSource;

'''
filter_node1758632570993 = sparkSqlQuery(glueContext, query = SqlQuery3351, mapping = {"myDataSource":Join_node1758549120300}, transformation_ctx = "filter_node1758632570993")

# Script generated for node customer curated
EvaluateDataQuality().process_rows(frame=filter_node1758632570993, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758550683427", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customercurated_node1758550950637 = glueContext.getSink(path="s3://isi123buck/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customercurated_node1758550950637")
customercurated_node1758550950637.setCatalogInfo(catalogDatabase="steadi",catalogTableName="customer_curated")
customercurated_node1758550950637.setFormat("json")
customercurated_node1758550950637.writeFrame(filter_node1758632570993)
job.commit()