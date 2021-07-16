import sqlite3
import os



def create_task_table():
    conn = sqlite3.connect("memory.db")
    cursor = conn.cursor()
    try:
        cursor.execute("""CREATE TABLE researches
                    (research text, 
                    path_to_research text, 
                    status text)
                """)
        
        print("Table created!")
    except:
        print("Table already exist")
    return cursor, conn  


def getPathsToDirs(path_dir):
    directory_list = list()
    for root, dirs, _ in os.walk(path_dir, topdown=False):
        for name in dirs:
            directory_list.append(os.path.join(root, name))

    return directory_list


def getTaskName(dir):
    dir = dir.split("/")
    dir = dir[len(dir)-1]
    return dir


def put_task_in_table(name, dir, status, cursor):
    parts = [(name, dir, status)]
    
    try:
        cursor.executemany("INSERT INTO researches VALUES (?,?,?)", parts)
        
        print("Done!")
    except:
        print("Something wrong!")

def deleteTable(conn):

    cursorObj = conn.cursor()

    cursorObj.execute('DROP table researches')

    conn.commit() 


def main():
    path = "/home/mkiit/patients/"
    condition = ["Not processed", "Processing for 3D", "Waiting for ML", "Done"]

    cursor, conn = create_task_table()

    directories = getPathsToDirs(path)

    for dir in directories:
        name = getTaskName(dir)
        put_task_in_table(name, dir, condition[0], cursor)
        conn.commit()

    with conn:    
        cursor = conn.cursor()    
        cursor.execute("SELECT * FROM researches")
        rows = cursor.fetchall()
    
    for row in rows:
        print(row)

    deleteTable(conn)



main()
