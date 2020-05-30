from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators

# Defining the plugin class
class UnaffiPlugin(AirflowPlugin):
    name = "unaffi_plugin"
    operators = [
        operators.AwinTransactionOperator,
    ]