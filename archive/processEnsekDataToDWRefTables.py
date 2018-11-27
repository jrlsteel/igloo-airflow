############ REQUIREMENTS ####################
# sudo apt-get install python-pip
# sudo apt-get install libpq-dev
# sudo pip install psycopg2
# sudo pip install sqlalchemy
# sudo pip install sqlalchemy-redshift
##############################################

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

#>>>>>>>> MAKE CHANGES HERE <<<<<<<<<<<<<
DATABASE = "dbname"
USER = "username"
PASSWORD = "password"
HOST = "host"
PORT = ""
SCHEMA = "public"      #default is "public"

####### connection and session creation ##############
connection_string = "redshift+psycopg2://%s:%s@%s:%s/%s" % (USER,PASSWORD,HOST,str(PORT),DATABASE)
engine = sa.create_engine(connection_string)
session = sessionmaker()
session.configure(bind=engine)
s = session()
SetPath = "SET search_path TO %s" % SCHEMA
s.execute(SetPath)
###### All Set Session created using provided schema  #######

################ write queries from here ######################
query = "SELECT * FROM added_trip limit 2;"
rr = s.execute(query)
all_results =  rr.fetchall()

def pretty(all_results):
    for row in all_results :
        print("row start >>>>>>>>>>>>>>>>>>>>")
        for r in row :
            print (" ----" , r)
        print ("row end >>>>>>>>>>>>>>>>>>>>>>")


pretty(all_results)


########## close session in the end ###############
s.close()