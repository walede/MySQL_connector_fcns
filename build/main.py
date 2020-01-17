import mysql_db
import tables
import mysql_schema
from datetime import datetime as DT
if __name__ == "__main__":

    #define a table schema
    t_name = 'Users'
    twitter_tables = {
        f'{t_name}' :
            f"""CREATE TABLE {t_name}(
                id BIGINT PRIMARY KEY,
                user_name VARCHAR(255) NOT NULL,
                created_at DATETIME DEFAULT NOW(),
                description VARCHAR(1000),
                verified BOOLEAN,
                follower_count INT,
                friend_count INT,
                location VARCHAR(255)
            );"""
    }

    #connect to the MySQL database 
    cnx = mysql_schema.mysql_connect(user,password) #user and password
    cursor = cnx.cursor()

    #create a database and select it.
    test_db = 'Tester'
    mysql_db.create_database(cursor, test_db)
    mysql_db.use_database(cursor, test_db)
    
    #create a table and perform CRUD statements
    tables.create_table(cursor, twitter_tables)

    #inserts - single and bulk
    insert_s = f"INSERT INTO {t_name} VALUES(1,'egg','{DT.now().strftime('%Y-%m-%d %H:%M:%S')}','hello', False, 4, 4, 'Land down under')"

    bulk_insert = (f"INSERT INTO {t_name} VALUES(2,'hatchling','{DT.now().strftime('%Y-%m-%d %H:%M:%S')}','hello',True,6,9,'Wales');"
                f"INSERT INTO {t_name} VALUES(3, 'bird','{DT.now().strftime('%Y-%m-%d %H:%M:%S')}',"
                f"'hello', False, 10000, 9, 'Space');"
                f"INSERT INTO {t_name} VALUES(4, 'velo','{DT.now().strftime('%Y-%m-%d %H:%M:%S')}',"
                f"'Hi', True, 10000, 100, 'SoHo');")
    tables.insert_into_table(cursor, t_name, insert_s, cnx)
    tables.bulk_insert_into_table(cursor, bulk_insert, cnx)

    #queries
    query = f"SELECT * FROM {t_name}"
    my_q = tables.query_table(cursor, t_name, query, cnx)
    query_2 = f"SELECT * FROM {t_name} WHERE location='SoHo';"
    my_q = tables.query_table(cursor, t_name, query_2, cnx)

    #updates - single and bulk
    update = f"UPDATE {t_name} SET verified=True WHERE follower_count>=5000;"
    update_2 =(f"UPDATE {t_name} SET friend_count=10 WHERE verified=True;"
              f"UPDATE {t_name} SET description= 'No more dinners at SoHo'"
              f"WHERE location='SoHo';")
    tables.update_table(cursor, t_name, update, cnx)
    my_q = tables.query_table(cursor, t_name, query, cnx)
    tables.bulk_update_from_table(cursor, update_2, cnx, mute=False)
    my_q = tables.query_table(cursor, t_name, query, cnx)

    #deletes - single and bulk
    del_op = f"DELETE FROM {t_name} WHERE location='SoHo';"
    tables.del_from_table(cursor, t_name, del_op, cnx)
    my_q = tables.query_table(cursor, t_name, query, cnx)
    del_op2 = (f"DELETE FROM {t_name} WHERE verified=False;"
              f"DELETE FROM {t_name} WHERE user_name='velo';")
    tables.bulk_del_from_table(cursor, del_op2, cnx)
    my_q = tables.query_table(cursor, t_name, query, cnx)

    #CUD function - a combination of inserts, updates, deletes
    cud_s = (f"INSERT INTO {t_name} VALUES(4, 'velo','{DT.now().strftime('%Y-%m-%d %H:%M:%S')}',"
            f"'Hi', True, 10000, 100, 'SoHo');"
            f"INSERT INTO {t_name} VALUES(1, 'egg','{DT.now().strftime('%Y-%m-%d %H:%M:%S')}',"
            f"'hello', False, 4, 4, 'Land down under');"
            f"UPDATE {t_name} SET description= 'No more dinners at SoHo'"
            f"WHERE location='SoHo';")
    del_all = f"DELETE FROM {t_name};"
    tables.bulk_CUD_table(cursor, cud_s, cnx)
    my_q = tables.query_table(cursor, t_name, query, cnx)
    tables.CUD_table(cursor, t_name, del_all, cnx)

    #delete the database
    mysql_db.drop_database(cursor, test_db)

    #close the cursor and the connection to the server
    cursor.close()
    cnx.close()    
