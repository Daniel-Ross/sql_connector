import sqlalchemy
from dotenv import dotenv_values
from urllib import parse


config = dotenv_values()

SYNAPSE_SERVER_URL = config["SYNAPSE_SERVER_URL"]
STAGING_SERVER_URL = config["STAGING_SERVER_URL"]
DRIVER = "ODBC+Driver+18+for+SQL+Server"
CRM_DATABASE = config["CRM_DATABASE"]
STAGING_DATABASE = config["STAGING_DATABASE"]
SYNAPSE_STAGING = config["SYNAPSE_STAGING"]
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
                                Server=tcp:{SYNAPSE_SERVER_URL},1433;
                                Database={CRM_DATABASE};
                                Uid={AD_USER};
                                Pwd={AD_PWD};
                                Encrypt=yes;
                                TrustServerCertificate=no;Connection Timeout=30;
                                Authentication=ActiveDirectoryPassword"""

    views_connection_string = f"""Driver={{ODBC Driver 18 for SQL Server}};
                                Server=tcp:{SYNAPSE_SERVER_URL},1433;
                                Database={SYNAPSE_STAGING};
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
    staging_engine = sqlalchemy.create_engine(staging_connection_string, pool_pre_ping=True)
    return {
        "dynamics": crm_engine,
        "staging": staging_engine,
        "synapse_views": views_engine,
    }
