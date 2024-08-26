import logging
import os
from dotenv import load_dotenv
load_dotenv(".env")
logger = logging.getLogger(__name__)


def set_env_var(var_name, value):
    os.environ[var_name] = value


def env_var_exists(var_name):
    """
    Checks if an environment variable named var_name exists.
    """
    return var_name in os.environ


def check_if_file_exists(file):
    """
    Check if file exists at the specified path
    """
    return os.path.isfile(file)


def get_env_var(var_name, prefix=None, is_required=True, expected_value=None):
    """
    Function to retrieve an Environment variable safely. If not set, it will raise an error.
    """
    if prefix:
        var_name = f"{prefix}_{var_name}"

    if is_required and not env_var_exists(var_name):
        if expected_value:
            raise ValueError(
                f"{var_name} environment variable not found expecting values: [{','.join(expected_value)}]"
            )
        else:
            raise ValueError(f"{var_name} environment variable not found")

    value = os.getenv(var_name)

    if value is not None:
        value = value.strip()

    if expected_value:
        if value not in expected_value:
            raise ValueError(
                f"Got a {var_name} value of {value} was expecting one of: [{','.join(expected_value)}]"
            )

    return value


def deserialize(input_data):
    """
    deserialize input data by cleaning and formatting the text
    """
    return f"[{input_data.replace('}{', '},{')}]"


def map_entity_type(items):
    
    if "facets" in items:
        if "symlinks" in items["facets"]:
            if "TABLE" in items["facets"]["symlinks"]["identifiers"][0]["type"]:
                mapper_key = "Table"
                return mapper_key
        elif "s3a://" in items["namespace"]: 
            if "symlinks" in items["facets"]:
                for identifier_type in items["facets"]["symlinks"]["identifiers"]:
                    if "TABLE" in identifier_type["type"]:
                        mapper_key = "Table"
                        return mapper_key
            else:
                mapper_key = "Container"
                return mapper_key
        elif "file" in items["namespace"]:
            mapper_key = "Container"
            return mapper_key
        else:
            logger.warn("Unrecognized metadata data source received. Expecting Table or Container!!")
            logger.warn("Skipping the facet")
            pass
    else:
        logger.info("Missing mandatory object called facet!!. Skipping this request")
        mapper_key = None
        return mapper_key


def get_output_handler(logger=None, severity=None):
    """
    Helper function to define a how to handle output. If a logger is provided, along with the corresponding severity,
    the logger will used. Otherwise, will print to console
    """
    if logger is not None:
        if severity.lower() not in ["critical", "error", "warning", "info", "debug"]:
            raise ValueError(
                "severity must be one of 'critical', 'error', 'warning', 'info', 'debug'. "
                "Value provided instead: {}".format(str(severity))
            )

        return getattr(logger.severity.lower())
    else:
        return print
