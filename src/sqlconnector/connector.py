import sqlalchemy
from dotenv import dotenv_values
from urllib import parse


config = dotenv_values()

SYNAPSE_SERVER_URL = "consumerworkspace.sql.azuresynapse.net"
STAGING_SERVER_URL = "aop-crm-sqlsrv.database.windows.net"
DRIVER = "ODBC+Driver+18+for+SQL+Server"
CRM_DATABASE = "dataverse_glynlyon_glynlyon2"
STAGING_DATABASE = "aop-crm-sql-02-staging"
SYNAPSE_STAGING = "consumer-sql-staging"
USER = config["DB_USER"]
PASSWD = config["DB_PASS"]
AD_USER = config["AD_UID"]
AD_PWD = config["AD_PWD"]


def create_connections():
    """
    Create a SqlAlchemy connection to both the Dynamics and staging databases.

    :return: An engine instance for both Dynamics and staging database.
    """
    crm_connection_string = f"""Driver={{ODBC Driver 18 for SQL Server}};
                                Server=tcp:consumerworkspace-ondemand.sql.azuresynapse.net,1433;
                                Database=dataverse_glynlyon_glynlyon2;
                                Uid={AD_USER};
                                Pwd={AD_PWD};
                                Encrypt=yes;
                                TrustServerCertificate=no;Connection Timeout=30;
                                Authentication=ActiveDirectoryPassword"""

    views_connection_string = f"""Driver={{ODBC Driver 18 for SQL Server}};
                                Server=tcp:consumerworkspace-ondemand.sql.azuresynapse.net,1433;
                                Database=consumer-sql-staging;
                                Uid={AD_USER};
                                Pwd={AD_PWD};
                                Encrypt=yes;
                                TrustServerCertificate=no;
                                Connection Timeout=30;
                                Authentication=ActiveDirectoryPassword"""

    staging_connection_string = f"""mssql+pyodbc://{USER}:{PASSWD}@{STAGING_SERVER_URL}/{STAGING_DATABASE}?driver={DRIVER}"""
    crm_connection_params = parse.quote_plus(crm_connection_string)
    views_connection_params = parse.quote_plus(views_connection_string)

    crm_engine = sqlalchemy.create_engine(
        "mssql+pyodbc:///?odbc_connect=%s" % crm_connection_params
    )
    views_engine = sqlalchemy.create_engine(
        "mssql+pyodbc:///?odbc_connect=%s" % views_connection_params
    )
    staging_engine = sqlalchemy.create_engine(
        staging_connection_string, pool_pre_ping=True
    )
    return {
        "dynamics": crm_engine,
        "staging": staging_engine,
        "synapse_views": views_engine,
    }
