import mysql.connector
import csv
import sys
import boto3
from messytables import CSVTableSet, type_guess, \
  types_processor, headers_guess, headers_processor, \
  offset_processor, any_tableset

# A table set is a collection of tables:
def csvParse(csv_file_path):
    fh = open(csv_file_path, 'rb')
    # Load a file object:
    table_set = CSVTableSet(fh)
    row_set = table_set.tables[0]
    # guess header names and the offset of the header:
    offset, headers = headers_guess(row_set.sample)
    row_set.register_processor(headers_processor(headers))
    # add one to begin with content, not the header:
    row_set.register_processor(offset_processor(offset + 1))
    # guess column types:
    types = type_guess(row_set.sample, strict=True)
    row_set.register_processor(types_processor(types))
    return row_set, headers, offset, types

def transformHeaderString(header_name):
    return header_name

def transformHeaderType(header_type):
    if str(header_type) == 'String':
        return 'TEXT'
    elif str(header_type) == 'Integer':
        return 'INTEGER'
    else:
        return 'TEXT'

def generateInsertSQL(table_name, headers, types):
    insert_sql = 'INSERT INTO ' + table_name + '('
    for col in headers:
        insert_sql = insert_sql + transformHeaderString(col) + ', '
    insert_sql = insert_sql[:len(insert_sql)-2] + ') VALUES ('
    for i in range(len(headers)):
        #insert_sql = insert_sql + ' %s::' + transformHeaderType(types[i]) + ', '
        insert_sql = insert_sql + ' %s, '
    return insert_sql[:len(insert_sql)-2] + ')'

def generateCreateTableSQL(table_name, headers, types):
    create_table_sql = 'CREATE TABLE ' + table_name + '('
    for i in range(len(headers)):
        create_table_sql = create_table_sql + ('' + transformHeaderString(headers[i])) + ' ' + ('' + transformHeaderType(types[i])) + ', '
    return create_table_sql[:len(create_table_sql)-2] + ')'

row_set, headers, offset, types = csvParse(sys.argv[1])

create_table_sql = generateCreateTableSQL('SSTABLE', headers, types);
'''
outputDB = mysql.connector.connect(
  host=sys.argv[2],
  user=sys.argv[3],
  password=sys.argv[4]
  database=sys.argv[5]
)
'''
outputDB = mysql.connector.connect(
  user=sys.argv[2],
  database=sys.argv[3]
)
table_name = 'SSTABLE'
outputCursor = outputDB.cursor(prepared=True)
outputCursor.execute('DROP TABLE IF EXISTS ' + table_name)
outputCursor.execute(create_table_sql)

insert_sql = generateInsertSQL(table_name, headers, types)
row_count = 0;

for row in row_set:
    row_count = row_count+1
    if row_count > 10000:
        exit
    param_tuple = ()
    for cell in row:
        param_tuple = param_tuple + (cell.value,)
    outputCursor.execute(insert_sql, param_tuple)

outputCursor.close()
