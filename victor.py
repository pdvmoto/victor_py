#
# victor.py : enter a query to inspect vector(s) from oracle-database ...
#
# background:
#   based on What is our vector victor? (yes, that movie from 1980)
#
# Stern Warning: 
#   This tool uses "dynamic SQL", 
#   and is Thus susceptible to SQL-Injections.
#   (hey, this tool IS exactly that: SQL-Injection,
#    Deal With It. ;-)
#
# todo:
# - replace hardcoded connect-strings by conn-name
# - also output to file e.g. victor.out ( but pipe-tee a.out works fine)
# - allow "run" or run-10 withtout stopping at every record
# - allow silent or scripted run: dont require keyboard input
# - devise some automated testing, and test more extensively
# - Graceful Exit on SQL-error (gently report ORA-00942 etc..).
# - enable use with psycopg2 as well
# - display subsequent columns if the cursor contains more afer the vector
# - remove need for quotes around arg[1] (nah...)
# - additional hash-total to allow equality-check between vectors
# - lots more, but not prio atm
#

import os
import sys
import array
import oracledb 

print ( ' ' ) 
print ( '--------- What is my vector Victor ? ---------- ' )
print ( ' ' ) 

# ---- chop off semicolon ---- 

def chop_off_semicolon ( qrystring ):

  retval = str.rstrip ( qrystring )
  n_last = len(retval) - 1

  if retval [ n_last ] == ';':
    retval = retval[:-1] 
    # print( 'sql stmnt chopped: [', retval, ']' )

  return retval # ---- chopped off semincolon ---- 


# --- the output handle: need this to convert vector to list ------ 

def output_type_handler(cursor, metadata):
    if metadata.type_code is oracledb.DB_TYPE_VECTOR:
        return cursor.var(metadata.type_code, arraysize=cursor.arraysize,
                          outconverter=list)

# ------ start of main ---------- 

# useful to know where we connect...

sql_show_conn="""
select 2 as ordr
     , 'version : ' || banner_full as txt
from v$version 
union all
select 1
     , 'user    : '|| user || ' @ ' || global_name|| ' ' as txt
FROM global_name     gn
order by 1
"""

err_no_list="""
..
.. Warning: 
..
.. Query result is not a vector/list.
.. Please provide a Query that returns a List (vector) as first column.
..
.. Example: Select vector from table ;
..
"""

# ------ Connect to the oracle database, and print conn-info -----

ora_conn = oracledb.connect(user="scott", password="tiger",
                              host="localhost", port=1521, service_name="freepdb1")

print ('Your Connection is: ' )
  
cursor = ora_conn.cursor()
for row in cursor.execute ( sql_show_conn ):
    print( '.. ', row[1] ) 

# print(' ..<-- Connection ' )

# prepare to handle vectors -> lists 
# sigh, do I really have to specify this... ?
ora_conn.outputtypehandler = output_type_handler


# --- check arguments ----- 

n = len(sys.argv)

# Arguments passed
# print("Name of Python script:", sys.argv[0])
# print("Total arguments passed:", n)
# print("Arguments passed:")
# for i in range(1, n):
#   print('..arg ' , i, ': [', sys.argv[i], ']')

# now start sql and real work

# sql_for_vector=""" select vect from img_vector  where id = 1 """
# sql_for_vector=""" select img_vector from vec_img_vect where id = 1 """

# if arg1: use it as SQL..
if len(sys.argv) == 2:
  sql_for_vector = sys.argv[1] 
else:
  print ( ' ' )
  sql_for_vector = input ( "Query to select vector(s)... SQL > " )

sql_for_vector = chop_off_semicolon ( sql_for_vector ) 

print ( ' ' )
print ( 'processing...' )
print ( ' ' )

rowcnt = 0
cursor = ora_conn.cursor()
for row in cursor.execute( sql_for_vector):

  rowcnt = rowcnt + 1
  # print(row)
  # f_inspect_obj ( 'row[0]', row[0] ) 
  # f_inspect_obj ( 'row[1] ', row[1] ) 
  # print ( '..row: ', rowcnt )

  if isinstance( row[0], list):

    print ( '.. Vector (list) Found:' )
    print ( '..   length          : ', len(row[0]) )
    print ( '..   type of elem[0] : ', type ( row[0][0]), '.' )

    n_elem = 0
    n_hashtot = 0.0

    for elem in row[0]:
      print ( '.. [', n_elem, ']: ', f"{elem:+5,.12}" ) 
      n_hashtot = n_hashtot + elem
      n_elem = n_elem + 1

    print ( '..<- vector(list) printed ' )
    print ( '      nr_of_elements      : ', n_elem )
    print ( '      sumtotal of element : ', n_hashtot ) 
    print ( '      row_nr in query     : ', rowcnt )

    try:
      print ( ' ' )
      hit_enter = input ( "hit enter for next, Contr-C to quit ... " )
      print ( ' ' )
      # suggestion: 0=run forever, n>0=run n records, enter=> n=1

    except KeyboardInterrupt:
      print ( ' ' )
      print ( '.. Interrupt, stopped cursor-fetching' ) 
      break

  else:

    print ( err_no_list )
    break

  # end if
# end for cursor


print ( ' ' ) 
print ( '.. Query was          : [', sql_for_vector, ']' )
print ( '.. nr rows processed  : ', rowcnt )
print ( ' ' ) 
print ( '------------- Roger that. Victor is Over and Out. ----------' ) 
print ( ' ' ) 

