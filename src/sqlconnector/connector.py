import sqlalchemy
from dotenv import load_dotenv
from urllib import parse
import os

load_dotenv()

SYNAPSE_SERVER_URL = os.getenv("SYNAPSE_SERVER_URL")
STAGING_SERVER_URL = os.getenv("STAGING_SERVER_URL")
DRIVER = "ODBC+Driver+18+for+SQL+Server"
CRM_DATABASE = os.getenv("CRM_DATABASE")
STAGING_DATABASE = os.getenv("STAGING_DATABASE")
SYNAPSE_STAGING = os.getenv("SYNAPSE_STAGING")
USER = os.getenv("DB_USER")
PASSWD = os.getenv("DB_PASS")
AD_USER = os.getenv("AD_UID")
AD_PWD_LOCAL = os.getenv("AD_PWD_LOCAL")


def create_connections(
    synapse_server_url=SYNAPSE_SERVER_URL,
    crm_database=CRM_DATABASE,
    ad_user=AD_USER,
    ad_pwd_local=AD_PWD_LOCAL,
    user=USER,
    passwd=PASSWD,
    staging_server_url=STAGING_SERVER_URL,
    staging_database=STAGING_DATABASE,
):
    """
    Create a SqlAlchemy connection to both the Dynamics and staging databases.

    :return: An engine instance for both Dynamics and staging database.
    """
    crm_connection_string = f"""Driver={{ODBC Driver 18 for SQL Server}};
                                Server=tcp:{synapse_server_url},1433;
                                Database={crm_database};
                                Uid={ad_user};
                                Pwd={ad_pwd_local};
                                Encrypt=yes;
                                TrustServerCertificate=no;Connection Timeout=30;
                                Authentication=ActiveDirectoryPassword"""

    views_connection_string = f"""Driver={{ODBC Driver 18 for SQL Server}};
                                Server=tcp:consumerworkspace-ondemand.sql.azuresynapse.net,1433;
                                Database=consumer-sql-staging;
                                Uid={ad_user};
                                Pwd={ad_pwd_local};
                                Encrypt=yes;
                                TrustServerCertificate=no;
                                Connection Timeout=30;
                                Authentication=ActiveDirectoryPassword"""

    staging_connection_string = f"""mssql+pyodbc://{user}:{passwd}@{staging_server_url}/{staging_database}?driver={DRIVER}"""
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


if __name__ == "__main__":
    connections = create_connections()
    print("Done")
