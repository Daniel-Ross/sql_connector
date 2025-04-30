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
AD_PWD = os.getenv("AD_PWD")


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


if __name__ == "__main__":
    connections = create_connections()
    print("Done")
