
import sqlite3
 
conn = sqlite3.connect("memory.db") # или :memory: чтобы сохранить в RAM
cursor = conn.cursor()
 
# Создание таблицы
cursor.execute("""CREATE TABLE researches
                  (research text, path_to_research text, status text)
               """)