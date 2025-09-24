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

# Script generated for node customer trusted
customertrusted_node1758725713107 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://isi123buck/customer/trusted/"], "recurse": True}, transformation_ctx="customertrusted_node1758725713107")

# Script generated for node accelerometer landing
accelerometerlanding_node1758725714993 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://isi123buck/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometerlanding_node1758725714993")

# Script generated for node customer_trusted
customer_trusted_node1758549004647 = glueContext.create_dynamic_frame.from_catalog(database="steadi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1758549004647")

# Script generated for node accelerometer_landing
accelerometer_landing_node1758549007732 = glueContext.create_dynamic_frame.from_catalog(database="steadi", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1758549007732")

# Script generated for node SQL Query
SqlQuery3192 = '''
select user, timestamp, x, y, z from acc_landing as al
join cust_trusted  as ct
on al.user = ct.email;
'''
SQLQuery_node1758550309242 = sparkSqlQuery(glueContext, query = SqlQuery3192, mapping = {"cust_trusted":customertrusted_node1758725713107, "acc_landing":accelerometerlanding_node1758725714993}, transformation_ctx = "SQLQuery_node1758550309242")

# Script generated for node accelerometer trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758550309242, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758550683427", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometertrusted_node1758550950637 = glueContext.getSink(path="s3://isi123buck/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometertrusted_node1758550950637")
accelerometertrusted_node1758550950637.setCatalogInfo(catalogDatabase="steadi",catalogTableName="accelerometer_trusted")
accelerometertrusted_node1758550950637.setFormat("json")
accelerometertrusted_node1758550950637.writeFrame(SQLQuery_node1758550309242)
job.commit()