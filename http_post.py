import sqlite3
import time
import json

def connect_to_database(fileDb):
    try:
        conn = sqlite3.connect(fileDb)
        cursor = conn.cursor()
        print("Connected secsessfully!")
    except:
        print("Connection failed!")

    return conn, cursor


def get_tasks(cursor):
    tasks = list()
    cursor.execute("SELECT research FROM researches")
    resultSet = cursor.fetchall()

    for r in resultSet:
        tasks.append(str(r).replace('(', '').
        replace(')', '').
        replace(',', '').
        replace("'", "")
        )
    # print(tasks)
    return tasks


def get_paths(tasks, cursor):
    tasksAndPaths = dict()
    
    for t in tasks:
        cursor.execute("SELECT path_to_research FROM researches WHERE research = ?", [str(t)])
        resultSet = cursor.fetchall()
        tasksAndPaths.update({t:str(resultSet).replace('(', '').
        replace(')', '').
        replace('[', '').replace(']', '').
        replace('"', ''). 
        replace("'", "")[:-1].
        split(", ")}
        )
        # print(tasksAndPaths)
        # print(len(str(resultSet).replace('(', '').
        # replace(')', '').
        # replace('[', '').replace(']', '').
        # replace('"', ''). 
        # replace("'", "")[:-1].
        # split(", ")))
        
    return tasksAndPaths


def create_json(tasks, tasksAndPaths):
    
    for t in tasks:
        jsondict = dict()
        jsondict.update({t : tasksAndPaths[t]})
        with open("tasks_json/" + t + ".json", "w") as fp:
            json.dump(jsondict , fp, sort_keys=True, indent=4)     
    

def main():
    conn, cursor = connect_to_database("tasks.db")
    tasks = get_tasks(cursor)
    tasksAndPaths = get_paths(tasks, cursor)
    create_json(tasks, tasksAndPaths)
    



if __name__ == "__main__": 
    start_time = time.time()
    main()
    print("--- Running time: %s seconds ---" % (time.time() - start_time))
