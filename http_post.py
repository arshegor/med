import sqlite3
import time
import sys
import json
import os

def connect_to_database(fileDb):
    try:
        conn = sqlite3.connect(fileDb)
        cursor = conn.cursor()
        print("Connected secsessfully!")
    except:
        print("Connection failed!")

    return conn, cursor


def waiting():
    sys.stdout.write('''  Waiting for new tasks.                             
    ''')
    time.sleep(.5)
    sys.stdout.write('\r')
    sys.stdout.flush()
    
    sys.stdout.write('''  Waiting for new tasks..                            
    ''')
    time.sleep(.5)
    sys.stdout.write('\r')
    sys.stdout.flush()

    sys.stdout.write('''  Waiting for new tasks...                           
    ''')
    time.sleep(.5)
    sys.stdout.write('\r')
    sys.stdout.flush()


def get_tasks(tasks, cursor):
    flag = False
    try:
        cursor.execute("SELECT research FROM researches")
        resultSet = cursor.fetchall()

        

        for r in resultSet:
            r = str(r).replace('(', '').replace(')', '').replace(',', '').replace("'", "")
            if r not in tasks:
                flag = True
                tasks.append(r)
                
        return tasks, flag
    except:
        waiting()
        return tasks, flag
        


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


def get_task_time(t, cursor):
    cursor.execute("SELECT create_time FROM researches WHERE research = ?", [str(t)])
    result = cursor.fetchall()
    result = str(result).replace('(', '').replace(')', '').replace('[', '').replace(']', '').replace('"', ''). replace("'", "")[:-1] 
    result = float(result)
    
    return(result)


def set_task_status(task, status, conn, cursor):
    cursor.execute("UPDATE researches SET status = ? WHERE research = ?", [status, task])
    conn.commit()
    print("UPDATE status done!")


def get_task_status(task, conn, cursor):
    cursor.execute("SELECT status From researches WHERE research = ?", [task])
    return str(cursor.fetchall()).replace('(', '').replace(')', '').replace('[', '').replace(']', '').replace('"', ''). replace("'", "")[:-1] 


def create_json(t, tasksAndPaths):
    jsondict = dict()
    jsondict.update({"name" : t})
    jsondict.update({"paths" : tasksAndPaths[t]})
    with open("tasks_json/" + t + ".json", "w+") as fp:
        json.dump(jsondict , fp, sort_keys=True, indent=4)
    fp.close()
    print("json ", "tasks_json/" + t + ".json", " created!")    
    

def main():
    # try:
    #     os.mkdir("tasks_json")
    # except:
    #     pass
    time.sleep(10)
    tasks = list()
    conn, cursor = connect_to_database("tasks.db")
    while True:
        tasks, newTasks = get_tasks(tasks, cursor)
        
        tasksAndPaths = get_paths(tasks, cursor)
        
        
        for t in tasks:
            sended = False
            # sys.stdout.write(str(time.time() - get_task_time(t, cursor)) + "\n")
            # sys.stdout.write('\r')
            # sys.stdout.write(str(time.time() - get_task_time(t, cursor)))
            # sys.stdout.flush()
            if time.time() - get_task_time(t, cursor) > 30 and get_task_status(t, conn, cursor) == "Not processed":
                create_json(t, tasksAndPaths)
                set_task_status(t, "Sended to NN", conn, cursor)
            tasks, newTasks = get_tasks(tasks, cursor)
            # if not newTasks:
            #     waiting()
    



if __name__ == "__main__": 
    main()
    
