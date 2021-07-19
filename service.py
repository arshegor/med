import glob
import os
import pydicom
import sqlite3
import time 

class Task():
    # Constructor
    def __init__(self, taskName, status) -> None:
        self.taskName = taskName
        self.paths = list()
        self.status = status

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
                    path_to_research text, 
                    status text)
                """) #3 fields
        
        print("Table created!")
    except:
        print("Table already exist")

    return cursor, conn   

# Writing a task into a table
def put_task_into_table(task, conn, cursor):
    parts = [(task.taskName, str(task.paths), task.status)]
    try:
        cursor.executemany("INSERT INTO researches VALUES (?,?,?)", parts)
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

    cursor.execute('DROP table researches')

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
    name_for_task = (meta['InstitutionAddress'] + " "
    + meta['StudyDate'] + " "
    + meta['PatientsName']
    + meta['PatientsBirthDate'] + " "
    + meta['SUID']
    )
    
    return name_for_task.replace(" ", "_").replace(".", "_").replace(",", "")


# Getting meta data from an DCM image
def get_dicom_meta(file):
    meta = dict()
    if file.SeriesDescription == "LUNG":
        try:
            meta.update({'InstitutionAddress' : file.InstitutionAddress})
            meta.update({'PatientsName' : str(file.PatientName)})
            meta.update({'SUID': file[0x0020, 0x000e].value})
            meta.update({'PatientsBirthDate':file.PatientBirthDate})
            meta.update({'StudyDate' : file.StudyDate})
        except:
            pass

    return meta


# Searching for a task in the task list
def search_task(taskName, tasks):
    for t in tasks:
        if t.taskName == taskName:
            return t


def main(path):
    # Creating a task table
    start_time = time.time()
    cursor, conn = create_task_table()
    print("--- Time of DB creating: %s seconds ---" % (time.time() - start_time))

    path = path + '/'

    # Creating tasks
    start_time = time.time()

    # List of task states
    condition = ["Not processed", "Processing for 3D", "Waiting for ML", "Done"]

    files = list()
    tasks = list()
    tasksNames = list()

    # Choosing a directory for search DCM files
    os.chdir(path)
    
    for file in sorted(glob.glob("*/*/*/*/*/*/*.dcm")): #Selecting all DCM files in directory
        if file not in files:
            files.append(file)
            dcm = load_dicom(file)
            meta = get_dicom_meta(dcm)
            print(file)
            if len(meta) != 0:
                taskName = create_task_name(meta)
                print(taskName)
                if taskName not in tasksNames:
                    onePathForTask = path+file
                    tasksNames.append(taskName)

                    task = Task(taskName, condition[0])
                    task._append_path(onePathForTask)
                    tasks.append(task)
                else:
                    task = search_task(taskName, tasks)
                    task._append_path(path + file)
            else:
                print("To remove!")
    
    # Writing tasks to the task table
    for t in tasks:
        try:
            deleteTask(t, cursor)  
            print("Delete is done!")     
            put_task_into_table(t, conn, cursor)
        except:
            if t.taskName in tasksNames:
                print("Delete is wrong, but all is OK!")
                put_task_into_table(t, conn, cursor)
    conn.commit()
    print("--- Time of task creating add writing to the task table: %s seconds ---" % (time.time() - start_time)) 
    
    # print_table(conn)
    
    # taskToDelete = search_task("Voronezh_Russia_20210618_MOCHALOV_V_N_19780331", tasks)
    # deleteTask(taskToDelete, conn, cursor)
    #set_task_status(tasks[0], condition[2], conn, cursor)
    # deleteTable(conn)
    

if __name__ == "__main__": 
    start_time = time.time()
    main("/mnt/data/dicoogle")
    print("--- Running time: %s seconds ---" % (time.time() - start_time))  
    
