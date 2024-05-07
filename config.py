import os
from configparser import ConfigParser
from typing import List

env_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), '.env')

def read_config_boolean(config, key: str, value: str) -> bool:

    result: bool = False

    if config[key][value] is not None:
        result = bool(config[key][value].lower() == "true")

    return result

class CrawlerSettings:

    def __init__(self):
        self.__read_settings()

    def __read_settings(self):
        config = ConfigParser()
        config.read(env_file)
        self.__default_reel_batch_size = int(config["crawler"]["DEFAULT.REEL.BATCH.SIZE"])
        self.__base_reel_location = str(config["crawler"]["BASE.REEL.LOCATION"])
        self.__export_json_data = read_config_boolean(config, "crawler", "EXPORT.JSON.DATA")
        self.__enable_email_notifications = read_config_boolean(config, "crawler", "ENABLE.EMAIL.NOTIFICATIONS")
        self.__1960_target_extensions = str(config["crawler"]["1960.target.extensions"])
        self.__1970_target_extensions = str(config["crawler"]["1970.target.extensions"])
        self.__1980_target_extensions = str(config["crawler"]["1980.target.extensions"])
        self.__1990_target_extensions = str(config["crawler"]["1990.target.extensions"])

    def __get_default_reel_batch_size(self) -> int:
        return self.__default_reel_batch_size
    def __get_base_reel_location(self) -> str:
        return self.__base_reel_location
    def __get_export_json_data(self) -> bool:
        return self.__export_json_data    
    def __get_enable_email_notifications(self) -> bool:
        return self.__enable_email_notifications
      
    default_reel_batch_size = property(__get_default_reel_batch_size)
    base_reel_location = property(__get_base_reel_location)
    export_json_data = property(__get_export_json_data)
    enable_email_notifications = property(__get_enable_email_notifications)

    def getCrawlerDefaultExtensions(self, census_year: int) -> List[str]:

        result = ['jpg']

        raw_extension_string = None

        if census_year == 1960:
            raw_extension_string = self.__1960_target_extensions
        elif census_year == 1970:
            raw_extension_string = self.__1970_target_extensions
        elif census_year == 1980:
            raw_extension_string = self.__1980_target_extensions
        elif census_year == 1990:
            raw_extension_string = self.__1990_target_extensions

        if(raw_extension_string.find(',') > -1):
            result = []
            for extension in raw_extension_string.split(','):
                if len(extension) > 0:
                    result.append(extension.strip())
        else:
            result = [raw_extension_string.strip()]

        return result

class PickerSettings:

    def __init__(self) -> None:
        self.__read_settings()

    def __read_settings(self):
        config = ConfigParser()
        config.read(env_file)

        self.__default_snowball_storage = int(config["picker"]["DEFAULT.TB.SNOWBALL.STORAGE"])
        self.__default_fill_threshold = float(config["picker"]["DEFAULT.FILL.THRESHOLD"])
    
    def _get_default_snowball_storage(self) -> int:
        return self.__default_snowball_storage

    def _get_default_fill_threshold(self) -> float:
        return self.__default_fill_threshold

    default_snowball_storage = property(_get_default_snowball_storage)
    default_fill_threshold = property(_get_default_fill_threshold)


class DatabaseSettings:

    def __init__(self) -> None:
        self.__read_settings()

    def __read_settings(self):
        config = ConfigParser()
        config.read(env_file)

        self.__host = str(config["database"]["PGSQL.HOST"])
        self.__port = int(config["database"]["PGSQL.PORT"])
        self.__user = str(config["database"]["PGSQL.USER"])
        self.__database = str(config["database"]["PGSQL.DATABASE"])
        self.__schema = str(config["database"]["PGSQL.SCHEMA"])

    def _get_host(self) -> str:
        return self.__host
    
    def _get_port(self) -> int:
        return self.__port

    def _get_user(self) -> str:
        return self.__user

    def _get_database(self) -> str:
        return self.__database
    
    def _get_schema(self) -> str:
        return self.__schema

    def _pg_connection_string(self) -> str:
        return f'dbname={self.database} user={self.user} port={self.port} host={self.host} options=-c search_path={self.schema}'

    host = property(_get_host)
    port = property(_get_port)
    user = property(_get_user)
    database = property(_get_database)
    schema = property(_get_schema)
    pg_connection_string = property(_pg_connection_string)


    def use_dev_schema(self):
        self.__update_schema_value("ips_dev")
    def use_test_schema(self):
        self.__update_schema_value("ips_test")
    def use_live_schema(self):
        self.__update_schema_value("ips_live")

    def refresh(self):
        self.__read_settings()

    def __update_schema_value(self, value):
        
        config = ConfigParser()
        config.read(env_file)
        config.set('database', 'PGSQL.SCHEMA',value)

        with open(env_file, 'w') as configfile:
            config.write(configfile)

    @property 
    def current_database_environment(self):
        
        current_schema = "UNKNOWN"

        if self.__schema.find('dev') > 0:
            current_schema = "DEV"
        if self.__schema.find('test') > 0:
            current_schema = "TEST"
        if self.__schema.find('live') > 0:
            current_schema = "PRODUCTION"
        
        return current_schema

        

class ReportSettings:

    def __init__(self) -> None:
        self.__read_settings()

    def __read_settings(self):
        config = ConfigParser()
        config.read(env_file)
        self.__report_output_dir = str(config["reporting"]["default.report.output.dir"]) 
        self.__temp_dir = str(config["reporting"]["temp.dir"])

    @property
    def report_output_dir(self) -> str:
        return self.__report_output_dir

    @property 
    def temp_dir(self) -> str:
        return self.__temp_dir

    

class AWSSettings:

    def __init__(self) -> None:
        self.__read_settings()


    def __read_settings(self):
        config = ConfigParser()
        config.read(env_file)
       
        self.__aws_snowball_s3_bucket = config["aws"]["aws.snowball.s3.bucket"]
        self.__aws_access_key = config["aws"]["aws.access.key"]
        self.__aws_secret_token = config["aws"]["aws.secret.token"]
        self.__aws_region = config["aws"]["aws.region"]

       

    @property
    def snowball_bucket(self) -> str:
        return self.__aws_snowball_s3_bucket

    @property
    def access_key(self) -> str:
        return self.__aws_access_key
        
    @property
    def secret_token(self) -> str:
        return self.__aws_secret_token

    @property
    def region(self) -> str:
        return  self.__aws_region

