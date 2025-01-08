import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import EntityDetector
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node Sanctions Data
SanctionsData_node1734272505994 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://pii-dataset-tutorial-for-handling-sensitive-data/Sanctioned_List_By_Countries.csv"], "recurse": True}, transformation_ctx="SanctionsData_node1734272505994")

# Script generated for node Drop Phones' column
DropPhonescolumn_node1734293950334 = DropFields.apply(frame=SanctionsData_node1734272505994, paths=["phones"], transformation_ctx="DropPhonescolumn_node1734293950334")

# Script generated for node Detect Sensitive Data
detection_parameters = {"PERSON_NAME": [{
  "action": "REDACT",
  "actionOptions": {"redactText": "Hide"}
}]}

entity_detector = EntityDetector()
DetectSensitiveData_node1734294028895 = entity_detector.detect(DropPhonescolumn_node1734293950334, detection_parameters, "DetectedEntities", "HIGH")

# Script generated for node Updated Sanctions Data
EvaluateDataQuality().process_rows(frame=DetectSensitiveData_node1734294028895, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734293741160", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
UpdatedSanctionsData_node1734294088165 = glueContext.getSink(path="s3://pii-dataset-tutorial-for-handling-sensitive-data", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="UpdatedSanctionsData_node1734294088165")
UpdatedSanctionsData_node1734294088165.setCatalogInfo(catalogDatabase="pii-cleaned-db",catalogTableName="pii-data")
UpdatedSanctionsData_node1734294088165.setFormat("glueparquet", compression="snappy")
UpdatedSanctionsData_node1734294088165.writeFrame(DetectSensitiveData_node1734294028895)
job.commit()