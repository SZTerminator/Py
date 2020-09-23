#!python
import pandas as pd
import sqlite3
import sys
import requests as r 

conn = sqlite3.connect('data.db')
cursor = conn.cursor()

def write(link):
	response = r.get(link)
	with open("data.csv", "w") as f :
		f.write(response.text)
	

def csv2sql(csvPath, con, sep=','):

	df = pd.read_csv(csvPath, sep=sep)
	df.gender = df.gender.map({
	    'Female': 0,
	    'Male': 1
	})

	index_arr =  df[df.name.isnull() | df.last_name.isnull()].index
	df.drop(index_arr, axis=0, inplace=True)

	df.to_sql('t_user', con=conn, if_exists='replace')


def showTable(tableName):
	cursor.execute(f'select * from {tableName}')
	for row in cursor.fetchall():
		print(row)


def createUserTable():
	cursor.execute(''' 
		create table if not exists user(
			id integer primary key autoincrement,
			user_id integer,
			name varchar(128) not null,
			last_name varchar(128) not null,
			email varchar(128),
			gender integer,
			start_dttm datetime default curent_timestamp,
			end_dttm   datetime default (datetime('2999-12-31 23:59:59'))
		)

		''')

	cursor.execute(''' 
		create view if not exists v_user as
			select 
				id,
				user_id,
				name,
				last_name,
				email,
				gender
			from user 
			where current_timestamp between start_dttm and end_dttm;

		''')

def createNewUsers():

	cursor.execute(''' 
		create table if not exists newusers as
			select 
				t1.*
			from t_user t1
			left join v_user t2
			on t1.id = t2.user_id
			where t2.user_id is null;
	''')

def createDeletedUsers():
	cursor.execute(''' 
		create table if not exists deletedusers as
			select 
				t1.*
			from t_user t1
			left join v_user t2
			on t1.id = t2.user_id
			where t1.id is null;
	''')

def createUpdatedUsers():
	cursor.execute(''' 
		create table if not exists updatedusers as
			select 
				t1.*
			from t_user t1
			left join v_user t2
			on t1.id = t2.user_id
			where t2.user_id is not null;
	''')

def deletetables():
	cursor.execute("""
	drop table if exists t_user;
	""")
	cursor.execute("""
	drop table if exists v_users;
	""")
	cursor.execute("""
	drop table if exists user;
	""")
	cursor.execute("""
	drop table if exists d_users;
	""")
	cursor.execute("""
	drop table if exists newusers;
	""")

def delete():
	cursor.execute("""
	update user set end_dttm = current_timestamp where user_id in (select id from deletedusers) and end_dttm = datetime('2999-12-31 23:59:59')
	""")
	
	
	

link="https://raw.githubusercontent.com/HaykInanc/personCSV/master/perosons.csv"

filePath = sys.argv[1]
# write(link)
csv2sql(filePath, conn)
# showTable('t_user')
createUserTable()
createUpdatedUsers()
createDeletedUsers()
delete()
# deletetables()
	# print('Вы не ввели путь к файлу')


showTable("t_user")
