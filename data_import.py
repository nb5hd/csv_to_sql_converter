import yaml
import argparse
import pandas as pd
from sqlalchemy import create_engine
import os

# TODO create comprehensive unit tests
NECESSARY_KEY_SET =  set(["DATA_FILE", "DATABASE", "HOST", "PORT", "TABLE_NAME"])

def parse_filename(parser):
    parser.add_argument("config_filepath", help="Enter the filepath of the yaml file that is holding the database information")
    args = parser.parse_args()
    filepath = args.config_filepath
    if filepath[-5:]==".yaml":
        return filepath
    else:
        raise Exception("config_filepath must end in '.yaml'")


def read_config(file):
    with open(file, 'r') as stream:
        try:
            return yaml.load(stream)
        except yaml.YAMLError:
            raise IOError(file + " does not exist")


def check_keys(config_data):
    config_key_set = set(config_data.keys())
    if NECESSARY_KEY_SET.issubset(config_key_set):
        return True
    else:
        missing_set = NECESSARY_KEY_SET - config_key_set
        raise Exception("Missing keywords in config file: " + ', '.join(missing_set))


def retrieve_csv(config_data):
    try: 
        csv_as_df = pd.read_csv(config_data["DATA_FILE"])
    except IOError:
        raise IOError(config_data["DATA_FILE"] + " does not exist")
    csv_as_df.columns = [col.lower() for col in csv_as_df.columns]
    return csv_as_df


def get_database_auth():
    # db credentials are saved in environment variables
    db_auth_dict = {}
    db_auth_dict["DATABASE_NAME"] = str(os.getenv("DATABASE_NAME"))
    db_auth_dict["DATABASE_USER"] = str(os.getenv("DATABASE_USER"))
    db_auth_dict["DATABASE_PWD"] = str(os.getenv("DATABASE_PWD"))
    for k, v in db_auth_dict.items():
        if v == "None":
            raise Exception("Enivonmental Variable '" + k + "' does not exist.")  

    return db_auth_dict

def format_db_string(db_auth_dict, config_data):
    return config_data["DATABASE"] + '://' + db_auth_dict["DATABASE_USER"] + ':'+ db_auth_dict["DATABASE_PWD"] + '@' + config_data["HOST"] + ':' + str(config_data["PORT"]) + '/' + db_auth_dict["DATABASE_NAME"]


def main():
    parser = argparse.ArgumentParser()
    
    config_filename = parse_filename(parser) 

    config_data = read_config(config_filename)
    has_keys = check_keys(config_data)

    csv_as_df = retrieve_csv(config_data)
    
    db_auth_dict = get_database_auth()
    db_info = format_db_string(db_auth_dict, config_data)

    engine = create_engine(db_info)

    csv_as_df.to_sql(config_data["TABLE_NAME"], engine, if_exists='replace')
    
    print "Data has been successfully stored in TABLE {0} of DATABASE {1}".format(config_data["TABLE_NAME"], os.getenv("DATABASE_NAME"))

if __name__ == '__main__':
    main()
