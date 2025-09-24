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

# Script generated for node customer_landing
customer_landing_node1758540644843 = glueContext.create_dynamic_frame.from_catalog(database="steadi", table_name="customer_landing", transformation_ctx="customer_landing_node1758540644843")

# Script generated for node Data filter
SqlQuery2352 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null;
'''
Datafilter_node1758540762984 = sparkSqlQuery(glueContext, query = SqlQuery2352, mapping = {"myDataSource":customer_landing_node1758540644843}, transformation_ctx = "Datafilter_node1758540762984")

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(frame=Datafilter_node1758540762984, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758539297153", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_node1758540836786 = glueContext.getSink(path="s3://isi123buck/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1758540836786")
customer_trusted_node1758540836786.setCatalogInfo(catalogDatabase="steadi",catalogTableName="customer_trusted")
customer_trusted_node1758540836786.setFormat("json")
customer_trusted_node1758540836786.writeFrame(Datafilter_node1758540762984)
job.commit()