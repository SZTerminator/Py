#!./User/bin/python
import pandas as pd
import sqlite3, sys, os, time
import requests as r 

class Table:
	type = "table"
	def __init__(self, name, state = None):
		self.name = name 
		self.state = state
	def insert(self, keys, value):
		print(f"insert into {self.name} ({keys}) values {value} \n")
		cursor.execute(f"insert into {self.name} ({keys}) values{value}")
		return self
	def delete(self, statement = ""): 
		print(f"delete from {self.name} {statement}")
		cursor.execute(f"delete from {self.name} {statement}")
		return self
	def create(self): 
		cursor.execute(self.state)
		return self
	def drop(self, view = False):
		if view:
			cursor.execute(f"drop view if exists {self.name}")
		else:
			cursor.execute(f"drop table if exists {self.name}")
		return self
	def kill(self): 
		self.delete()
		self.drop()
		return self
	def recreate(self):
		self.delete()
		self.drop()
		self.create()
		return self
	def update(self, key, value, where): 
		print(f"update {self.name} set {key} = {value} {where} \n")
		cursor.execute(f"update {self.name} set {key} = {value} {where}")
		return self
	def fetch(self, line = "*"):
		cursor.execute(f"select {line} from {self.name}")
		return cursor.fetchall()
	def show(self):
		print(self.name + ": \n")
		for row in self.fetch("*"):
			print(row)
		return self
def csv2sql(csvPath, con, sep=','):
	df = pd.read_csv(csvPath, sep=sep)
	df.gender = df.gender.map({
	    'Female': 0,
	    'Male': 1       })
	index_arr =  df[df.name.isnull() | df.last_name.isnull()].index
	df.drop(index_arr, axis=0, inplace=True)
	df.to_sql('t_users', con=conn, if_exists='replace')
def qwe():
	select("t_users")
	for row in cursor.fetchall():
		user.insert(row[1],row[2],row[3],row[4],row[5])
	cursor.execute("delete from t_users")
	print("qwe executed")
def insert(file, t_key, p_key, keys, end_key):
	# keys = "id, name, last_name, email, gender"
	
	u_users.drop()
	t_users.drop()
	n_users.drop()
	v_users.drop(1)
	d_users.drop()

	csv2sql(file, conn)
	v_users.create()
	n_users.create()
	u_users.create()
	d_users.create()
	
	data = n_users.fetch(t_key + "," + keys)
	if data:
		# print("Data : \n",data)
		users.insert(p_key + "," + keys, str(data)[1:-1].replace("None","null")) # 1-st is before VALUES key-word | 2-nd is after VALUES key-word
	data = d_users.fetch(t_key + "," + keys)
	if data:
		ls = []
		for i in data:
			ls.append(i[0])
		users.update(end_key,"CURRENT_TIMESTAMP",f"where {p_key} in ({str(ls)[1:-1]})")
	
	data = u_users.fetch(t_key + "," + keys)
	if data:
		ls = []
		for i in data:
			ls.append(i[0])
		users.update(end_key,"CURRENT_TIMESTAMP",f"where {p_key} in ({str(ls)[1:-1]})")
		users.insert(p_key + "," + keys, str(data)[1:-1]) # 1-st is before VALUES key-word | 2-nd is after VALUES key-word
	time.sleep(1)
	v_users.drop(1).create()
def show_tables():
	try: users.show() 
	except: pass
	try: t_users.show() 
	except: pass
	try: n_users.show()
	except: pass
	try: v_users.show()
	except: pass
	try: u_users.show()
	except: pass
	try: d_users.show()
	except: pass
def update(file):
	insert(file, "id","user_id","name,last_name, email,gender", "end_dttm")

conn = sqlite3.connect('data.db')
cursor = conn.cursor()
commit = conn.commit

users   = Table("users", ''' create table if not exists users(
							id integer primary key autoincrement,
							user_id integer,
							name varchar(128) not null,
							last_name varchar(128) not null,
							email varchar(128),
							gender integer,
							start_dttm datetime default current_timestamp,
							end_dttm   datetime default (datetime('2999-12-31 23:59:59')))	''')
t_users = Table("t_users")
n_users = Table("n_users", ''' create table if not exists n_users as
								select      t_users.id, t_users.name, t_users.Last_name, t_users.email, t_users.gender
								from 		t_users 
								left join   v_users 
								on 			t_users.id = v_users.user_id
								where 		v_users.user_id is null    ''')
d_users = Table("d_users", ''' create table if not exists d_users as
								select      v_users.*
								from 		v_users 
								left join 	t_users 
								on 			t_users.id = v_users.user_id
								where 		t_users.id is null      ''')
u_users = Table("u_users", ''' create table if not exists u_users as
								select t_users.* from v_users 
								left join t_users on 
								v_users.user_id = t_users.id
								where t_users.id is not null   ''')
v_users = Table("v_users", ''' create view if not exists v_users as 
								select 
								id,       user_id,
								name,     last_name,
								email,    gender , start_dttm,end_dttm
								from users 
								where start_dttm < current_timestamp and current_timestamp < end_dttm     ''')

users.create()

file = sys.argv[1]
update(file)
commit()
del(cursor)
conn.close()