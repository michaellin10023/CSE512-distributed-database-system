#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    con = openconnection
    cur = con.cursor()
    cur.execute("SELECT MAX(%s), MIN(%s) FROM %s;" % (SortingColumnName, SortingColumnName, InputTable))
    max_val, min_val = cur.fetchone()
    numThreads = 5
    delta = float(max_val - min_val) / numThreads
    TEMP_TABLE_PREFIX = 'temp_table'
    thread_list = numThreads * [0]
    for i in range(numThreads):
        min_range = min_val + delta * i
        max_range = min_range + delta
        thread_list[i] = threading.Thread(target=sortFun, args=(InputTable,SortingColumnName,TEMP_TABLE_PREFIX
                                            ,min_range,max_range,i,openconnection,))
        thread_list[i].start()

    cur.execute("DROP TABLE IF EXISTS " + OutputTable)
    cur.execute("CREATE TABLE %s (LIKE %s INCLUDING ALL);" % (OutputTable, InputTable))
    for i in range(numThreads):
        thread_list[i].join()
        temp_table = TEMP_TABLE_PREFIX + str(i)
        cur.execute("INSERT INTO %s SELECT * FROM %s;" % (OutputTable, temp_table))

    cur.close()
    con.commit()

def sortFun(InputTable, SortingColumnName, TEMP_TABLE_PREFIX, min_range, max_range, i, openconnection):
    con = openconnection
    cur = con.cursor()
    temp_table_name = TEMP_TABLE_PREFIX + str(i)
    cur.execute("DROP TABLE IF EXISTS " + temp_table_name)
    cur.execute("CREATE TABLE %s (LIKE %s INCLUDING ALL);" % (temp_table_name, InputTable))
    if i == 0:
        cur.execute("INSERT INTO %s SELECT * FROM %s WHERE %s >= %f AND %s <= %f ORDER BY %s ASC;" 
            % (temp_table_name, InputTable, SortingColumnName, min_range, SortingColumnName, max_range, SortingColumnName))
    else:
        cur.execute("INSERT INTO %s SELECT * FROM %s WHERE %s > %f AND %s <= %f ORDER BY %s ASC;" 
            % (temp_table_name, InputTable, SortingColumnName, min_range, SortingColumnName, max_range, SortingColumnName))


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    con = openconnection
    cur = con.cursor()
    cur.execute("SELECT MAX(%s), MIN(%s) FROM %s" % (Table1JoinColumn, Table1JoinColumn, InputTable1))
    max_col_val1, min_col_val1 = cur.fetchone()
    # print(max_col_val1, min_col_val1)
    cur.execute("SELECT MAX(%s), MIN(%s) FROM %s" % (Table2JoinColumn, Table2JoinColumn, InputTable2))
    max_col_val2, min_col_val2 = cur.fetchone()
    # print(max_col_val2, min_col_val2)
    max_col_val = max(max_col_val1, max_col_val2)
    min_col_val = min(min_col_val1, min_col_val2)
    numThreads = 5
    delta = float(max_col_val - min_col_val) / numThreads
    cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'" % InputTable1)
    schema1 = cur.fetchall()
    # print(schema1)
    cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'" % InputTable2)
    schema2 = cur.fetchall()
    # print(schema2)
    thread_list = [0] * numThreads
    TEMP_TABLE1_PREFIX = "temp_table1"
    TEMP_TABLE2_PREFIX = "temp_table2"
    TEMP_OUTPUT_TABLE_PREFIX = "temp_output_table"
    
    for i in range(numThreads):
        min_range = min_col_val * i
        max_range = min_range + delta
        thread_list[i] = threading.Thread(target=joinFun, 
                                        args=(InputTable1,InputTable2,Table1JoinColumn,Table2JoinColumn,TEMP_TABLE1_PREFIX,
                                        TEMP_TABLE2_PREFIX,TEMP_OUTPUT_TABLE_PREFIX,schema1,schema2,min_range,max_range,
                                        i,openconnection))
        thread_list[i].start()
    cur.execute("DROP TABLE IF EXISTS %s" % OutputTable)
    cur.execute("CREATE TABLE %s (LIKE %s INCLUDING ALL);" % (OutputTable, InputTable1))
    cmd = "ALTER TABLE %s " % OutputTable
    for j in range(len(schema2)):
        if j != len(schema2) - 1:
            cmd += "ADD COLUMN %s %s," % (schema2[j][0], schema2[j][1])
        else:
            cmd += "ADD COLUMN %s %s;" % (schema2[j][0], schema2[j][1])
    cur.execute(cmd)
    input()
    for i in range(numThreads):
        thread_list[i].join()
        table1 = TEMP_TABLE1_PREFIX + str(i)
        table2 = TEMP_TABLE2_PREFIX + str(i)
        output_table = TEMP_OUTPUT_TABLE_PREFIX + str(i)
        cur.execute("INSERT INTO %s SELECT * FROM %s;" % (OutputTable, output_table))
    cur.close()
    con.commit()

def joinFun(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, TEMP_TABLE1_PREFIX, TEMP_TABLE2_PREFIX, TEMP_OUTPUT_TABLE_PREFIX, schema1, schema2, min_range, max_range, i, openconnection):
    con = openconnection	
    cur = con.cursor()
    table1 = TEMP_TABLE1_PREFIX + str(i)
    table2 = TEMP_TABLE2_PREFIX + str(i)
    output_table = TEMP_OUTPUT_TABLE_PREFIX + str(i)
    cur.execute("DROP TABLE IF EXISTS %s;" % table1)
    cur.execute("CREATE TABLE %s (LIKE %s INCLUDING ALL);" % (table1, InputTable1))
    cur.execute("DROP TABLE IF EXISTS %s;" % table2)
    cur.execute("CREATE TABLE %s (LIKE %s INCLUDING ALL);" % (table2, InputTable2))
    cur.execute("DROP TABLE IF EXISTS %s;" % output_table)
    cur.execute("CREATE TABLE %s (LIKE %s INCLUDING ALL);" % (output_table, InputTable1))
    cmd = "ALTER TABLE %s " % output_table
    for j in range(len(schema2)):
        if j != len(schema2) - 1:
            cmd += "ADD COLUMN %s %s," % (schema2[j][0], schema2[j][1])
        else:
            cmd += "ADD COLUMN %s %s;" % (schema2[j][0], schema2[j][1])
    cur.execute(cmd)
    if i == 0:
        cur.execute("INSERT INTO %s SELECT * FROM %s WHERE %s >= %f AND %s <= %f;" 
                        % (table1, InputTable1, Table1JoinColumn, min_range, Table1JoinColumn, max_range))
        cur.execute("INSERT INTO %s SELECT * FROM %s WHERE %s >= %f AND %s <= %f;" 
                        % (table2, InputTable2, Table2JoinColumn, min_range, Table2JoinColumn, max_range))
    else:
        cur.execute("INSERT INTO %s SELECT * FROM %s WHERE %s > %f AND %s <= %f;" 
                        % (table1, InputTable1, Table1JoinColumn, min_range, Table1JoinColumn, max_range))
        cur.execute("INSERT INTO %s SELECT * FROM %s WHERE %s > %f AND %s <= %f;" 
                        % (table2, InputTable2, Table2JoinColumn, min_range, Table2JoinColumn, max_range))
    
    cur.execute("INSERT INTO %s SELECT * FROM %s INNER JOIN %s ON %s.%s = %s.%s;" % 
                        (output_table, table1, table2, table1, Table1JoinColumn, table2, Table2JoinColumn))

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


