from airflow.plugins_manager import AirflowPlugin
import operators

# Defining the plugin class
class ETLPlugin(AirflowPlugin):
    name = "ETLplugin"
    operators = [
        operators.StagetoRedshiftOperator,
        operators.LoadDataOperator,
        operators.DataQualityOperator
    ]