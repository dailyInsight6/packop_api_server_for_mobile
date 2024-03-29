# SQL Server Database Connection Properties
import pyodbc
import os
import keymanager

server = keymanager.SQL_SERVER_ADDRESS
database = keymanager.DATABASE_NAME
username = keymanager.DATABASE_USERNAME
password = keymanager.DATABASE_PASSWORD

# Return the sql connection
def get_connection():
    driver = '{FreeTDS}'
    TDS_VERSION = 8.0
    connection = pyodbc.connect(
        'DRIVER={};SERVER={};PORT=1433;DATABASE={};UID={};PWD={};TDS_VERSION={}'.format(driver, server, database,
                                                                                        username, password,
                                                                                        TDS_VERSION))
    return connection


def close_connection(connection):
    # Commit the data
    connection.commit()

    # Close the connection
    connection.close()


def insert_data(connection, table_name, data: list):
    str = ""
    tmpstr = "?,"
    value_list = []

    # Get the sql cursor
    cursor = connection.cursor()

    for i in range(len(data)):
        str = str + tmpstr
        value_list.append(data[i])

    sql_query = str[:-1]
    sql_query = "Insert Into " + table_name + " Values(" + sql_query + ")"
    cursor.execute(sql_query, value_list)


def select_data(connection, table_name, target, condition: dict, date_condition: list = list(), order_by: bool = False):
    data = []
    # Get the sql cursor
    cursor = connection.cursor()

    sql_query = "select "
    for i, column_name in enumerate(target):
        if i == (len(target) - 1):
            sql_query = sql_query + column_name + " "
        else:
            sql_query = sql_query + column_name + ", "
    sql_query = sql_query + "from " + table_name

    if len(condition) > 0:
        sql_query = sql_query + " where 1=1"
        for key, value in condition.items():
            sql_query = sql_query + " and " + key + " = " + "'" + value + "'"
    
    if len(date_condition) > 0:
        sql_query = sql_query + " and date between CONVERT(datetime, '{}') and CONVERT(datetime, '{}')".format(date_condition[0], date_condition[1])

    if order_by:
        sql_query = sql_query + " order by date desc, time desc"
    # Execute the sql query
    result = cursor.execute(sql_query).fetchall()
    for row in result:
        data.append([x for x in row])
    return data

def custom_select_data(connection, sql_query):
    data = []
    # Get the sql cursor
    cursor = connection.cursor()
    # Execute the sql query
    result = cursor.execute(sql_query).fetchall()
    for row in result:
        data.append([x for x in row])
    return data


def update_data(connection, table_name, data: dict, condition_id):
    # Get the sql connection
    value_list = []
    cursor = connection.cursor()

    sql_query = "Update " + table_name + " Set "
    if len(data) > 0:
        for key in data:
            sql_query = sql_query + key + " = ?, "
        sql_query = sql_query[:-2]
        sql_query = sql_query + " where 1=1"
    if len(condition_id) > 0:
        for key in condition_id:
            sql_query = sql_query + " and " + key + " =?"

    # Execute the update query
    for key in data:
        value_list.append(data[key])
    for key in condition_id:
        value_list.append(condition_id[key])

    cursor.execute(sql_query, value_list)
