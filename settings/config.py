import os
from dotenv import load_dotenv

root_path = os.path.abspath(os.path.dirname(__file__))
path_env_file = os.path.join(root_path, '.env')

if os.path.exists(path_env_file):
    load_dotenv(dotenv_path=path_env_file)
else:
    load_dotenv()

SQL_DW_SERVER = os.environ.get("dw_server")
SQL_DW_DATABASE = os.environ.get("dw_database")
SQL_DW_USERNAME = os.environ.get("dw_username")
SQL_DW_PASSWORD = os.environ.get("dw_password")
ELASTIC_PASSWORD = os.environ.get("ELASTIC_PASSWORD")
CLOUD_ID = os.environ.get("CLOUD_ID")


