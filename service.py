import glob
import os
import pydicom
import sqlite3
import time 
import sys
import json

from time import sleep

# TODO:
# change all print() and stdout to logger class

class Task():
    # Constructor
    def __init__(self, taskName, status) -> None:
        self.taskName = taskName
        self.paths = list()
        self.status = status
        
        self.time = time.time()

    # Add the path to the file to the list of paths
    def _append_path(self, path):
        self.paths.append(path)
    
    # Print information about task 
    def _info(self):
        print("-----------------------------------------------------\r\n")
        print("CREATED TASK --------->\r\n")
        print("-----------------------------------------------------\r\n")
        print("Task name:\r\n" + self.taskName + "\r\n")
        print("-----------------------------------------------------\r\n")
        print("Paths:\r\n", self.paths, "\r\n")
        print("-----------------------------------------------------\r\n")
        print("Status:\r\n" + self.status + "\r\n")
        

# Creating a table with research tasks
def create_task_table():
    conn = sqlite3.connect("tasks.db")
    cursor = conn.cursor()

    
    try:
        cursor.execute("""CREATE TABLE researches 
                    (research text NOT NULL UNIQUE, 
                    path_to_research text, create_time datetime, 
                    status text)
                """) #3 fields
        
        print("Table created!")
    except:
        print("Table already exist")

    return cursor, conn   

# Writing a task into a table
def put_task_into_table(task, conn, cursor):
    parts = [(task.taskName, str(task.paths), task.time, task.status)]
    try:
        cursor.executemany("INSERT INTO researches VALUES (?,?,?,?)", parts)
        conn.commit()
        print("Inserting TASK done!")
    except:
        pass
    

# Setting a task status
def set_task_status(task, status, conn, cursor):
    cursor.execute("UPDATE researches SET status = ? WHERE research = ?", [status, task.taskName])
    conn.commit()
    print("UPDATE status done!")


# Table Output
def print_table(conn):
    print("\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n-----------------------------------------------------\r\n")
    print("DB tasks.db\r\n")
    print("-----------------------------------------------------\r\n")
    with conn:    
        cursor = conn.cursor()    
        cursor.execute("SELECT * FROM researches")
        rows = cursor.fetchall()
    
    for row in rows:
        print(row)
    print("-----------------------------------------------------\r\n\r\n\r\n")

    conn.commit()


# Deleting a task table
def deleteTable(conn):
    cursor = conn.cursor()

    cursor.execute('DROP TABLE researches')

    conn.commit() 
    print("Table DELETED!")


# Deleting something task in task table
def deleteTask(task, conn, cursor):
    cursor.execute("DELETE FROM researches WHERE research = ?", [task.taskName])
    conn.commit()
    print("Task deletion %s completed" % task.taskName)


# Loading DCM images
def load_dicom(path):
    return pydicom.dcmread(path)


# Formation of the task name
def create_task_name(meta):
    name_for_task = (meta['StudyDate'] + " "
    + meta['PatientsName']
    + meta['PatientsBirthDate'] + " "
    + meta['SUID']
    )
    
    return name_for_task.replace(" ", "_").replace(".", "_").replace(",", "")


# Getting meta data from an DCM image
def get_dicom_meta(file):
    meta = dict()
    # print(file)
    if str(file.SeriesDescription) == 'LUNG':  
        # meta.update({'InstitutionAddress' : file.InstitutionAddress})
        meta.update({'PatientsName' : str(file.PatientName)})
        meta.update({'SUID': file[0x0020, 0x000e].value})
        meta.update({'PatientsBirthDate':file.PatientBirthDate})
        meta.update({'StudyDate' : file.StudyDate})
        
    return meta


# Searching for a task in the task list
def search_task(taskName, tasks):
    for t in tasks:
        if t.taskName == taskName:
            return t


# Scanning for new files
def scan_files(path, files, tasksNames, newTasks):
    flag = False
    
    condition = ["Not processed", "Processing for 3D", "Waiting for ML", "Done"]
    for file in sorted(glob.glob("*/*/*/*/*/*/*.dcm")): #Selecting all DCM files in directory
        if file not in files:
            flag = True
            files.append(file)
            dcm = load_dicom(file)
            meta = get_dicom_meta(dcm)
            # print(file)
            if len(meta) != 0:
                taskName = create_task_name(meta)
                # print(taskName)
                if taskName not in tasksNames:
                    onePathForTask = path+file
                    tasksNames.append(taskName)

                    task = Task(taskName, condition[0])
                    task._append_path(onePathForTask)
                    newTasks.append(task)
                else:
                    task = search_task(taskName, newTasks)
                    task._append_path(path + file)
            else:
                flag = True
                # print("To remove!")
        
    return flag, newTasks


# Post json to NN
def create_and_post_json(task):
    # url = 'localhost:8080'
    print()
    
    jsondict = dict()
    jsondict.update({'name' : task.taskName})
    jsondict.update({'paths' : task.paths})
    with open("/home/mkiit/tasks_json/" + task.taskName + ".json", "w") as fp:
        json.dump(jsondict, fp, sort_keys=True, indent=4)
            # r = requests.post(url, files=fp)
            # r.text     
    print(str(fp.name) + " created")

    return True


def new_tasks(conn, cursor):
    pass


# Waiting animation
def waiting():
    sys.stdout.write('''  Waiting for new.                            ''')
    time.sleep(.5)
    sys.stdout.write('\r')
    sys.stdout.flush()
    
    sys.stdout.write('''  Waiting for new..                           ''')
    time.sleep(.5)
    sys.stdout.write('\r')
    sys.stdout.flush()

    sys.stdout.write('''  Waiting for new...                          ''')
    time.sleep(.5)
    sys.stdout.write('\r')
    sys.stdout.flush()
    



def main(path):
    # Creating a task table
    start_time = time.time()
    cursor, conn = create_task_table()
    print("--- Time of DB creating: %s seconds ---" % (time.time() - start_time))

    path = path + '/'

    # Creating tasks
    start_time = time.time()
    # deleteTable(conn)
    # List of task states
    condition = ["Not processed", "Sended to ML", "Processing for 3D", "Done"]

    files = list()
    tasks = list()
    tasksNames = list()

    # Choosing a directory for search DCM files
    os.chdir(path)

    while True:
        isNewTasks, tasks = scan_files(path, files, tasksNames, tasks)
        
        
        if isNewTasks:
            # print("Я тут")
            # Writing tasks to the task table
            for t in tasks:    
                if t.status == condition[0]:
                    put_task_into_table(t, conn, cursor)
                          
            conn.commit()
            
            print()
        else:
            waiting()
                
            
            
    
            
    
    
    # print("--- Time of task creating add writing to the task table: %s seconds ---" % (time.time() - start_time)) 
    # taskToDelete = search_task("Voronezh_Russia_20210618_MOCHALOV_V_N_19780331", tasks)
    # deleteTask(taskToDelete, conn, cursor)
    #set_task_status(tasks[0], condition[2], conn, cursor)
    # deleteTable(conn)
    # print_table(conn)
        

if __name__ == "__main__": 
    main(sys.argv[0])

    
