from sqlalchemy.sql.expression import false, true
from datetime import datetime
import pandas as pd
import utils
from sqlalchemy import null, types, create_engine, exc
from sqlalchemy.exc import SQLAlchemyError, DatabaseError
from sqlalchemy.types import String
import sys
from os import getcwd, path
import readCfg
from datetime import datetime, date
import numpy as np
import cx_Oracle
#from pip import _internal
pd.options.mode.chained_assignment = None


# TODO REQUERIMIENTOS:
# La tabla de errores debe ser igual en estructura que la tabla de stg salvo que contiene además los campos: 'error' y 'error_date')
#TODO estandarizar las tablas de error para no sacar esas cols
# chunk_df.drop(columns=["update_date"], errors='ignore',inplace=True)
# chunk_df.drop(columns=["insert_date"], errors='ignore',inplace=True)
# chunk_df.drop(columns=["batch_id"], errors='ignore',inplace=True)

#TODO: apuntar a la paramétrica de prod
    
def run(conn_str_stg,conn_str_prc, order, schema_param):
    #conn_str = 'oracle+cx_oracle://' + conf.user + ':' + conf.password + '@' + conf.host + '/' + conf.sid #+ '&autocommit=true'

    #TEST
    order = 54
    #print(_internal.main(['list']))

    try:

        engine_stg = create_engine(conn_str_stg)
        conn_stg = engine_stg.connect().execution_options(stream_results=True)

        engine_prc = create_engine(conn_str_prc)
        conn_prc = engine_prc.connect().execution_options(stream_results=True)


        #cargo el df con los datos de la tabla paramétrica en orden
        df_parametric = utils.read_parametric_exact_order(pd, conn_prc, order, schema_param)
        if len(df_parametric) == 1:
            current_datetime = datetime.utcnow()
            #date_UTC = current_datetime #current_datetime.strftime("%Y-%m-%d %H:%M:%S")
            for a in range(len(df_parametric)):
                try:
                    query = df_parametric.iloc[a]['query_stg']
                    table_name = df_parametric.iloc[a]['t_stage']
                    table_insis = df_parametric.iloc[a]['t_insis']
                    batch_size =  df_parametric.iloc[a]['batch_size']
                    pk_table =  df_parametric.iloc[a]['pk']
                    table_trace_name =  df_parametric.iloc[a]['t_trace']
                    schema_stg = df_parametric.iloc[a]['s_stage']
                    schema_trace = df_parametric.iloc[a]['s_trace']
                    table_prc = df_parametric.iloc[a]['t_processed']
                    schema_prc = df_parametric.iloc[a]['s_processed']
                    schema_error = df_parametric.iloc[a]['s_error']

                    pk_table = pk_table.lower()
                except AttributeError:
                    print('Falta llenar un campo en la tabla paramétrica con el orden: ' + str(order))                                
                    exit('Se termina el proceso, corregir el dato faltante en la tabla paramétrica con el orden: ' + str(order))
           

            
            print('---------> '+ table_name)
            datetime_ejec, id_ejecucion = utils.get_id_datetime()
            cant_tabla_stg  = 0
            cant_tabla_prc  = 0
            cant_tabla_error  = 0
            # batch_size = 500
            for chunk_df in pd.read_sql(query, conn_stg,  chunksize=int(batch_size)):                
                if not chunk_df.empty:
                    cant_tabla_stg = cant_tabla_stg + len(chunk_df)
                    df_ETL = pd.DataFrame()
                    df_ETL = utils.read_etl_table(pd, conn_prc, table_insis, schema_param)
                    df_rules = pd.DataFrame()                    
                    default_value_array = []
                    if len(df_ETL) > 0:
                        for a in range(len(df_ETL)):
                            rules = ""
                            rules_default = ""
                            rule_array = []
                            etl_field = df_ETL.iloc[a]['field']
                            etl_rule_regex = df_ETL.iloc[a]['rule_regex']
                            etl_rule_field = df_ETL.iloc[a]['rule_field']
                            etl_rule_null = df_ETL.iloc[a]['rule_null']
                            etl_rule_equal = df_ETL.iloc[a]['rule_equal']
                            etl_rule_min = df_ETL.iloc[a]['rule_min']
                            etl_rule_max = df_ETL.iloc[a]['rule_max']
                            etl_rule_name = df_ETL.iloc[a]['rule_name']
                            etl_range_min = df_ETL.iloc[a]['range_min']
                            etl_range_max = df_ETL.iloc[a]['range_max']
                            etl_mandatory = df_ETL.iloc[a]['rule_mandatory']
                            etl_default_value = df_ETL.iloc[a]['default_value']
                            if df_ETL.iloc[a]['etl_mandatory'] == 0:
                                continue

                            #TODO ver como hacer: si no es null, tal cosa
                            rule_array = utils.read_rule(chunk_df, rule_array, etl_rule_regex, 'rule_regex')
                            rule_array = utils.read_rule(chunk_df, rule_array, etl_rule_equal, 'rule_equal')
                            rule_array = utils.read_rule(chunk_df, rule_array, etl_rule_min, 'rule_min')
                            rule_array = utils.read_rule(chunk_df, rule_array, etl_rule_max, 'rule_max')
                            rule_array = utils.read_rule(chunk_df, rule_array, etl_range_min, 'range_min')
                            rule_array = utils.read_rule(chunk_df, rule_array, etl_range_max, 'range_max')
                            rule_array = utils.read_rule(chunk_df, rule_array, etl_rule_null, 'rule_null')
                            
                            default_value_array = utils.read_rule(chunk_df, default_value_array, etl_default_value, 'default_value', etl_field)
                            #rule_array = utils.read_rule(rule_array, etl_mandatory, 'rule_mandatory')
                            #TODO ver como definir la lógica del null, si con AND o con OR
                            
                            if etl_default_value is None:
                                try:
                                    rules = ' & '.join(rule_array)                                
                                    df_rules_part = chunk_df[eval(rules)]                                    
                                    # else:                      
                                    #     df_rules_part = eval(rules)
                                except KeyError as error:
                                    print('Error en el campo: ' + error.args[0])                                
                                    exit('Verificar si es correcto el campo: "'+  error.args[0] + '" de la regla en la tabla ETL')

                                df_rules_part['error']='Campo ' + etl_field + ': ' + etl_rule_name
                                df_rules_part['error_date'] = current_datetime
                                if etl_mandatory == 0:
                                    df_rules_part['error_type'] = 'Negocio'
                                else:
                                    df_rules_part['error_type'] = 'Mandatoria API'

                                df_rules = pd.concat([df_rules, df_rules_part], axis=0)                          
                        #Inserto en la tabla de errores
                        dtyp = {c:types.VARCHAR(df_rules[c].astype('string').str.len().max()) for c in df_rules.columns[df_rules.dtypes == 'object'].tolist()}
                        df_rules.to_sql(table_insis.lower(), engine_stg, schema=schema_error, if_exists='append', index=False, dtype=dtyp)

                        cant_tabla_error = cant_tabla_error + len(df_rules.eval(pk_table).value_counts())
                        #TODO ver si sirve hacer el del del df
                        del df_ETL                       

                        #Hago un array con los id's
                        # df_id = df_rules[pk_table].value_counts()
                        df_id = df_rules[pk_table]
                        nmp=df_id.to_numpy()
                        #Saco los campos que no cumplen con las reglas
                        df_to_processed = chunk_df[~chunk_df.eval(pk_table).isin(nmp)]
                        # Aplico los valores default al DF
                        if len(default_value_array) > 0:
                            # rules_default = ' & '.join(default_value_array)
                            for r in default_value_array:
                                eval(r)                                
                    #La tabla no tiene rules
                    else:
                        df_to_processed = chunk_df

                    #Saco los nat y nan
                    df_to_processed = utils.nat_to_none(df_to_processed)  
                    df_errors = df_to_processed.copy()
                    #Renombro el campo de fecha de update
                    df_to_processed.rename(columns={'update_date': 's_update'}, inplace=True)
                    #Elimino las col. q no están en processed
                    df_to_processed.drop(columns=["insert_date"], errors='ignore',inplace=True)
                    df_to_processed.drop(columns=["batch_id"], errors='ignore',inplace=True)

                    column_list_names = df_to_processed.columns.tolist()
                            
                    cols_ins = df_to_processed.columns.tolist()
                    cols_ins_list  = ','.join(cols_ins)
                    for index, value in enumerate(column_list_names):
                        column_list_names[index] = ''.join((":",value))
                    #Armo la lista de columnas para el df de insert
                    column_list = ','.join(column_list_names)
                    
                    rejected_df = pd.DataFrame()
                    rejs = pd.DataFrame()                       

                    try:
                        if conf.usa_service_name:
                            dsnStr = cx_Oracle.makedsn(conf.host_prc, conf.port_prc, service_name=conf.service_name_prc)
                        else:                      
                            dsnStr = cx_Oracle.makedsn(conf.host_prc, conf.port_prc, conf.sid_prc)                          

                        conn_cx = cx_Oracle.connect(user=conf.user_prc, password=conf.password_prc, dsn=dsnStr)
                        cursor = conn_cx.cursor()
                        cursor.arraysize = 100000
                        
                        # test!
                        # table_prc = 'P_PEOPLE_T'
                        #Ejecuto el insert
                        cursor.executemany("insert INTO " + schema_prc +"."+ table_prc + " (" + cols_ins_list + ") values (" + column_list + ")", df_to_processed.values.tolist(), batcherrors = True)
                        
                        cant_tabla_prc = cant_tabla_prc + len(df_to_processed) - len(cursor.getbatcherrors())
                        #Agarro los reject y lo formateo para insertar en la tabla de errores
                        for errorObj in cursor.getbatcherrors():
                            #print("Row", errorObj.offset, "has error", errorObj.message)
                            rejs = df_errors.iloc[errorObj.offset]
                            row_df = pd.DataFrame([rejs])
                            row_df['error'] =  str(errorObj.message)
                            row_df['error_date'] = current_datetime
                            row_df['error_type'] = 'Contraint DB'

                            rejected_df = pd.concat([rejected_df, row_df], axis=0)

                        conn_cx.commit()
                        

                        if len(rejected_df) > 0:
                            try:
                                if conf.usa_service_name:
                                    dsnStr = cx_Oracle.makedsn(conf.host_stg, conf.port_stg, service_name=conf.service_name_stg)
                                else:                      
                                    dsnStr = cx_Oracle.makedsn(conf.host_stg, conf.port_stg, conf.sid_stg)                          

                                conn_cx = cx_Oracle.connect(user=conf.user_prc, password=conf.password_prc, dsn=dsnStr)
                                cursor = conn_cx.cursor()
                                cursor.arraysize = 100000
                                column_list_names = rejected_df.columns.tolist()                                
                                cols_ins = rejected_df.columns.tolist()
                                cols_ins_list  = ','.join(cols_ins)
                                for index, value in enumerate(column_list_names):
                                    column_list_names[index] = ''.join((":",value))
                                #Armo la lista de columnas para el df de insert
                                column_list = ','.join(column_list_names)
                                cursor.executemany("insert INTO " + schema_error +"."+ table_insis.upper() + " (" + cols_ins_list + ") values (" + column_list + ")", rejected_df.values.tolist())
                            except cx_Oracle.DatabaseError as exc:
                                err = exc.args
                                print("Oracle-Error-Code:", err[0].code)
                                print("Oracle-Error-Message:", err[0].message)
                            finally:
                                cursor.close()
                                conn_cx.close()




                        obj_cols = rejected_df.select_dtypes(include=[object]).columns.values.tolist() 
                        rejected_df.to_sql(table_insis.lower(), engine_stg, schema=schema_error, if_exists='append', index=False, dtype={c: String for c in obj_cols})




                        #dtyp = {c:types.VARCHAR(rejected_df[c].astype('string').str.len().max()) for c in rejected_df.columns[rejected_df.dtypes == 'object'].tolist()}
                        #rejected_df.to_sql(table_insis.lower(), engine_stg, schema=schema_error, if_exists='append', index=False, dtype=dtyp)


                        if not rejected_df.empty:
                            cant_tabla_error = cant_tabla_error + len(rejected_df.eval(pk_table).value_counts())
                    except cx_Oracle.DatabaseError as exc:
                        err = exc.args
                        print("Oracle-Error-Code:", err[0].code)
                        print("Oracle-Error-Message:", err[0].message)
                    finally:
                        cursor.close()
                        conn_cx.close()

            print('cantidad registros con error: ' + str(cant_tabla_error))
            print('cantidad registros en stg: ' + str(cant_tabla_stg))
            # cant_tabla_prc = cant_tabla_stg - cant_tabla_error
            print('cantidad registros en prc: ' + str(cant_tabla_prc))
            data_audit = [[id_ejecucion, table_insis, datetime_ejec, 'OK','OK',cant_tabla_prc, 'PRC'],
                        [id_ejecucion, table_insis, datetime_ejec, 'OK','OK',cant_tabla_stg, 'STG'],
                        [id_ejecucion, table_insis, datetime_ejec, 'OK','OK',cant_tabla_error, 'ERROR']]
            df_audit = pd.DataFrame(data_audit, columns = ['ID_EJECUCION', 'TABLA','FECHA', 'RESULTADO','ESTADO','REG_INS', 'TIPO_TABLA'])


            #Inserto en la tabla de auditorìa
            dtyp = {c:types.VARCHAR(df_audit[c].astype('string').str.len().max()) for c in df_audit.columns[df_audit.dtypes == 'object'].tolist()}
            df_audit.to_sql('auditoria', engine_prc, schema= schema_param, if_exists='append', index=False, dtype=dtyp)


    except SQLAlchemyError as e:
        sys.exit("Error en la conexión: "+  str(e.__dict__['orig']))
    

    finally:
        if 'table_name' in locals():
            print("Fin proceso tabla ==> " + table_name)
        else:
            print ("No hay datos para la tabla orden ==> " + str(order))
        conn_stg.close()
        engine_stg.dispose()
        conn_prc.close()
        engine_prc.dispose()
     


if __name__ == '__main__':
    conf = readCfg.Config()
    # if conf.ambiente == 'DB_TEST':
    #     conn_str_stg = 'oracle+cx_oracle://' + conf.user_stg + ':' + conf.password_stg + '@' + conf.host_stg + '/' + conf.sid_stg #+ '&autocommit=true'
    #     conn_str_prc = 'oracle+cx_oracle://' + conf.user_prc + ':' + conf.password_prc + '@' + conf.host_prc + '/' + conf.sid_prc #+ '&autocommit=true'
    # else:
    #     conn_str_stg = 'oracle+cx_oracle://'+ conf.user_stg + ':' + conf.password_stg + '@(DESCRIPTION = (LOAD_BALANCE=on) (FAILOVER=ON) (ADDRESS = (PROTOCOL = TCP)(HOST = ' + conf.host_stg + ')(PORT = ' + conf.port_stg + ')) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = ' + conf.service_name_stg + ')))'
    #     conn_str_prc = 'oracle+cx_oracle://'+ conf.user_prc + ':' + conf.password_prc + '@(DESCRIPTION = (LOAD_BALANCE=on) (FAILOVER=ON) (ADDRESS = (PROTOCOL = TCP)(HOST = ' + conf.host_prc + ')(PORT = ' + conf.port_prc + ')) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = ' + conf.service_name_prc + ')))'
                        

    schema_param = conf.schema_param
    engine = create_engine(conf.conn_str_prc, pool_pre_ping=True)
    try:
        conn = engine.connect().execution_options(stream_results=True)
        tables_list = utils.tables_to_proc(engine, schema_param)
    except SQLAlchemyError as e:
        sys.exit("Error en la conexión: "+  str(e.__dict__['orig']))
    except DatabaseError as e:
        sys.exit("Error en la conexión: "+  str(e.__dict__['orig']))
    finally:
        conn.close()
        engine.dispose()  
    

    print('Comienzo del proceso')
    for  i in tables_list:
        print('Procesando tabla: ' +  str(i))
        run(conf.conn_str_stg, conf.conn_str_prc, i, schema_param)
        print('Fin tabla: ' +  str(i))    
