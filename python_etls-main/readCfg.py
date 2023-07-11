import configparser
import os

class Config:

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('etl.ini')
        self.ambiente = config['AMBIENTE']['ambiente']
        # self.conn_str_stg = config[self.ambiente]['conn_str_stg']
        # self.conn_str_prc = config[self.ambiente]['conn_str_prc']
        self.schema_param = config['SCHEMA_PARAMETRICA']['schema']
        self.ambiente = config['AMBIENTE']['ambiente']
        # if self.ambiente == 'TEST':
        #     DB = 'DB_TEST'
        # else:
        #     DB = 'DB_PROD'
        #src
        self.conn_str_stg = config[self.ambiente]['conn_str_stg']
        self.conn_str_prc = config[self.ambiente]['conn_str_prc']
        self.usa_service_name = True if config[self.ambiente]['usa_service_name'] == 'si' else False

        self.host_stg = config[self.ambiente]['host_stg']
        self.user_stg = config[self.ambiente]['user_stg']
        self.password_stg = config[self.ambiente]['password_stg']
        self.port_stg = config[self.ambiente]['port_stg']
        self.sid_stg = config[self.ambiente]['sid_stg']
        self.oracle_client_path_stg = config[self.ambiente]['oracle_client_path_stg']
        self.service_name_stg = config[self.ambiente]['service_name_stg']
        #prc
        self.host_prc = config[self.ambiente]['host_prc']
        self.user_prc = config[self.ambiente]['user_prc']
        self.password_prc = config[self.ambiente]['password_prc']
        self.port_prc = config[self.ambiente]['port_prc']
        self.sid_prc = config[self.ambiente]['sid_prc']
        self.oracle_client_path_prc = config[self.ambiente]['oracle_client_path_prc']
        self.service_name_prc = config[self.ambiente]['service_name_prc']   
      

