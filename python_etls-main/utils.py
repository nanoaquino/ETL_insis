
from ntpath import join
import re
from sqlalchemy import exc, Integer, text
from datetime import datetime, date
from os import walk, getcwd, path, makedirs
import re
import numpy as np
import sqlalchemy
from sqlalchemy.sql.expression import null
import shutil
import glob
import cx_Oracle


#METODOS PARA ETL

def max_tables_bup(engine):
    ''' Devuelve el orden máximo de las tablas a procesar
    '''
    #Estado = 5 es cuando se realiza el update a la api
    try:
        sql = '''SELECT max(ORDEN) FROM INSIS_PEOPLE_V10_TRACE.PARAMETRICA
                                        WHERE  MIGRADA = 0'''

        result = engine.execute(sql)  
        row = result.fetchone()
        result.close()
        if len(row) == 1:
            return int(row[0])
        else:
            return 0
    except exc as e:
        print(e)        


def tables_to_proc(engine, schema_param):
    ''' Devuelve el orden de las tablas a procesar
    '''

    try:
        sql = "SELECT ORDEN FROM "+ schema_param + ".PARAMETRICA WHERE  MIGRAR = 1 ORDER BY 1"

        list = []

        for row in engine.execute(sql):
            list.append(row[0])

        return list

    except exc as e:
        print(e)                              

def create_field(engine, schema, table_name, field, data_type):
    '''
    Método que ejecuta un sp para crear una columna si no existe
    '''

    sql = " DECLARE " + \
            "v_column_exists number := 0; "+ \
            "v_owner VARCHAR2 (255) :=  '"+ schema + "';" + \
            "v_table VARCHAR2(255) :=  '"+ table_name + "';" + \
            "v_col VARCHAR2 (255) := '"+ field + "';" + \
            "v_type VARCHAR2 (255):= '"+ data_type + "';" + \
            "BEGIN " + \
            "Select count(*) into v_column_exists " + \
                "from ALL_TAB_COLUMNS " + \
                "where upper(column_name) = v_col " + \
                "and upper(table_name) = v_table " + \
                "and upper(owner) = v_owner; " + \
            "if (v_column_exists = 0) then " + \
            "    execute immediate 'alter table ' || v_owner || '.' || v_table || ' add (' || v_col ||' '||v_type||')'; "+ \
            "end if; " + \
            "END; " + \
            "/"

    try:    
        engine = engine.execution_options(autocommit=True)
        engine.execute(sql)
    except exc as e:
        print(e)
    # table_name = 'P_ROLE_BILLING_SITES'
    # try:
    #     dsnStr = cx_Oracle.makedsn(conf.host, conf.port, conf.sid)
    #     conn_cx = cx_Oracle.connect(user=conf.user, password=conf.password, dsn=dsnStr)
    #     cursor = conn_cx.cursor()
    #     cursor.callproc('INSIS_PEOPLE_V10.insert_col', [schema, table_name, field, data_type])
    #     # INSIS_PEOPLE_V10
    # except cx_Oracle.Error as error:
    #     print(error)
    # engine = engine.execution_options(autocommit=True)
    # sqlalchemy.text
    # engine.execute(sqlalchemy.text("CALL INSIS_PEOPLE_V10.insert_col(:p1, :p2, :p3, :p4)"), p1=schema, p2=table_name, p3=field, p4=data_type)
    # # engine.execute("CALL INSIS_PEOPLE_V10.insert_col ?, ?, ?, ?", [schema, table_name, field, data_type])

    # conn = db.raw_connection()
    # cursor = conn.cursor()
    # cursor.callproc("INSIS_PEOPLE_V10.insert_col ?, ?, ?, ?", [schema, table_name, field, data_type])
    # # fetch result parameters
    # cursor.fetchall()
    # cursor.close()
    # conn.commit()
    # # CREATE OR REPLACE PROCEDURE "insert_col" (v_owner IN VARCHAR2 DEFAULT 'INSIS_PEOPLE_V10_FK', v_table IN VARCHAR2 DEFAULT 'P_ROLE_BILLING_SITES', v_col IN VARCHAR2 DEFAULT 'S_UPDATE', v_type VARCHAR2 DEFAULT 'date')

   
def read_parametric_exact_order(pd, conn, order, schema_param):
    ''' carga un df con la tabla paramética en el orden
    '''
    #Estado = 5 es cuando se realiza el update a la api
    #TODO sacar la tabla de test.
    try:
        # SQL_Query = pd.read_sql('''SELECT S_STAGE, S_PROCESSED, S_INSIS, T_STAGE, T_PROCESSED, 
        #                         T_INSIS, ORDEN, PRIORIDAD, PATH, QUERY, PK, HEATER, USR_STAGE, 
        #                         PWD_STAGE, AUT_STAGE, USR_PROCESSED, PWD_PROCESSED, AUT_PROCESSED,
        #                         USR_INSIS, PWD_INSIS, AUT_INSIS, AUTORREFERENCIAL, ESTADO, BATCH_SIZE,T_TRACE,
        #                         URL_INS, QUERY_UPD, T_REJECT, QUERY_STG FROM ''' + schema_param +'''.PARAMETRICA
        #                             WHERE  MIGRAR = 1 AND ORDEN ='''  + str(order) , conn)        
        SQL_Query = pd.read_sql('''SELECT * FROM ''' + schema_param +'''.PARAMETRICA
                                    WHERE  MIGRAR = 1 AND ORDEN ='''  + str(order) , conn) 
    except exc as e:
        print(e)

    return pd.DataFrame(SQL_Query)


def query_stg(schema_stg, table_name):
    sql = 'SELECT * FROM ' + schema_stg + '.' + table_name
    return sql



def read_etl_table(pd, conn, table_name, schema_trace):
    #Leo los reg. de la tabla ETL segun la tabla

    SQL_Query = pd.read_sql("SELECT * FROM "+ schema_trace +".ETL WHERE TABLA = '" + table_name.upper() + "' ORDER BY FIELD", conn)        
 

    return pd.DataFrame(SQL_Query)

def is_date_matching(date_str):
    '''Función que devuelve una fecha valida, o fecha actual
    '''
    try:
        s_date = datetime.strptime(date_str, '%d/%m/%y')
        return s_date
    except ValueError:
        if str(date_str).lower == 'today':
            today = date.today()
            date_str = datetime.strptime(today, '%d/%m/%y')
        return date_str

def is_number_matching(rule_val):

    try:
        return int(rule_val)        
    except ValueError:
        return rule_val





def intTryParse(value):
    try:
        return int(value), True
    except ValueError:
        return value, False




def read_rule(df, rule_array, etl_rule, type_rule, etl_field = null):
    '''Función que arma un array para filtrar el dataframe
    '''
    if etl_rule is not None:
        rule_array_etl = etl_rule.split('=')
        print('Ejecutando regla: ' + type_rule)
        if len(rule_array_etl) > 0:

            if type_rule == 'default_value':

                # if type(rule_array_etl[0]) is str:
                #     rule_array.append("chunk_df['"+ etl_field.lower()+"'].fillna('" + rule_array_etl[0] + "')")
                # if type(rule_array_etl[0]) is int:
                rule_array.append("df_to_processed['"+ etl_field.lower()+"'].fillna(" + rule_array_etl[0] + ",inplace=True)") 
                  


            if type_rule == 'rule_regex':
                rule_array.append("(~chunk_df['"+ rule_array_etl[0].lower()+"'].str.contains('"+ rule_array_etl[1]+"', na=False))")
            if type_rule == 'rule_equal':

                # if isinstance(rule_array_etl[0], str):
                #     rule_array.append("(chunk_df['"+ rule_array_etl[0].lower()+"'].str.lower()=='" + rule_array_etl[1].lower()+"')")
                # else:
                #     rule_array.append("(chunk_df['"+ rule_array_etl[0].lower()+"']==" + rule_array_etl[1] + ")")
                if df[rule_array_etl[0]].dtype == 'int64':
                    term0 = "(chunk_df['"+ rule_array_etl[0].lower()+"']==" 
                else:
                    term0 = "(chunk_df['"+ rule_array_etl[0].lower()+"'].str.lower()=="
                
                # if isinstance(rule_array_etl[1], str):
                val, is_num = intTryParse(rule_array_etl[1])
                if is_num:
                    term1 = str(val) +")"                        
                    # rule_array.append("(chunk_df['"+ rule_array_etl[0].lower()+"'].str.lower()=='" + rule_array_etl[1].lower()+"')")
                else:
                    term1 = "'" + rule_array_etl[1].lower()+"')" 

                rule_array.append(term0 + term1)

            if type_rule == 'rule_min': 
                rule_array.append("chunk_df['"+ rule_array_etl[0].lower()+"'] > chunk_df['"+ rule_array_etl[1].lower()+"']")
            if type_rule == 'rule_max': 
                rule_array.append("chunk_df['"+ rule_array_etl[0].lower()+"'] < chunk_df['"+ rule_array_etl[1].lower()+"']")                
            if type_rule == 'range_min':
                if isinstance(is_number_matching(rule_array_etl[1]), int):
                    rule_array.append("(chunk_df['"+ rule_array_etl[0].lower()+"'] < "+ rule_array_etl[1] + ")")
                # if isinstance(is_number_matching(rule_array_etl[1]), str):
                #     rule_array.append("(chunk_df['"+ rule_array_etl[0].lower()+"'] < "+ rule_array_etl[1] + ")")
                else:
                    r_min = is_date_matching(rule_array_etl[1])
                    #TODO validar si el campo del df es fecha
                    rule_array.append("chunk_df['"+ rule_array_etl[0].lower()+"'] < '"+ r_min + "'" )
            if type_rule == 'range_max':
                if isinstance(is_number_matching(rule_array_etl[1]), int):
                    rule_array.append("(chunk_df['"+ rule_array_etl[0].lower()+"'] > "+ rule_array_etl[1] + ")")
                else:
                    r_max = is_date_matching(rule_array_etl[1])
                    #TODO validar si el campo del df es fecha
                    rule_array.append("chunk_df['"+ rule_array_etl[0].lower()+"'] > '"+ r_max + "'" )
            if type_rule == 'rule_null':
                    rule_array.append("(~chunk_df['"+ rule_array_etl[0].lower()+"'].isnull())")

            '''
            lograr esto
            (~chunk_df['user_email'].str.contains('^[a-za-z0-9\\.\\+_-]+@[a-za-z0-9\\._-]+\\.[a-za-z]*$', na=False)) & (~chunk_df['user_email'].isnull())
            '''
    return rule_array
 
#chunk_df[chunk_df['name'].str.contains('^A.*') & chunk_df['man_comp']==1]

#FIN METODOS PARA ETL

def nat_to_none(df):
    '''Función que reemplaza losNat por none en las colúmnas del data frame
    '''
    #reemplaza los nat por none
    #df.fillna(None,  inplace = True)
    df = df.replace({np.nan: None})
    for (columnName, columnData) in df.iteritems():        
        #reemplaza los Nat por None en las columnas tipo datetime
        if df[columnName].dtype.name == 'datetime64[ns]':
            df[columnName] = df.eval(columnName).astype(object).where(df.eval(columnName).notnull(), None)

    return df


def get_id_datetime():
    '''Crea una id con la fecha y hora actual
    '''
    today = datetime.now()
    id_datetime = join( today.strftime('%Y%m%d')+ str(today.hour).zfill(2) + str(today.minute).zfill(2)+ str(today.second).zfill(2))
  
    return today, id_datetime

def get_dst_datetime(dst):
    '''Crea una carpeta con la fecha y hora actual y mueve los json procesados
    '''
    today = datetime.now()
    dst_datetime = join(dst, today.strftime('%Y%m%d')+ str(today.hour).zfill(2) + str(today.minute))
  
    # Creo el dir si no existe
    makedirs(dst_datetime, exist_ok=True)
    return dst_datetime