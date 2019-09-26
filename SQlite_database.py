#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 14:10:16 2019

@author: matteo
"""

import numpy as np
import sqlite3


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    
    except sqlite3.Error as e:
        print(e)
        return None

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except sqlite3.Error as e:
        print(e)
        
        
def insert_into_tables(conn,insert,dic):
    """
    Insert data in the tables
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    try:
        cur.execute(insert, (dic))
    except sqlite3.Error as e:
         print(e)
        
             
def main_sql(database):
 
    sql_commodities_table = """ CREATE TABLE IF NOT EXISTS commodities (
            Cset nvarchar(20),
            Region nvarchar(20),
            CommName nvarchar(128) PRIMARY KEY  NOT NULL,
			CommDesc nvarchar(244),
			Unit nvarchar(20),
			LimType nvarchar(20),
			CTSLvl nvarchar(20),
			PeakTS nvarchar(20),
			Ctype nvarchar(20)
            ); """
    
    sql_technologies_table = """ CREATE TABLE IF NOT EXISTS processes (
            Sets nvarchar(20),
            Region nvarchar(20) ,
            TechName  nvarchar(128) NOT NULL,
			TechDesc nvarchar(244),
			Tact nvarchar(20),
			Tcap nvarchar(20),
			Tslvl nvarchar(20),
			PrimaryCG nvarchar(20),
			Vintage nvarchar(20),
            PRIMARY KEY ( Region, TechName)
            ); """
    
    # create a database connection
    conn = create_connection(database)
    if conn is not None:
        # create tables
        create_table(conn, sql_commodities_table)
        create_table(conn, sql_technologies_table)
    else:
        print("Error! cannot create the database connection.")
        
def main_sql_FI_T(database):

    sql_FI_T_table = """ CREATE TABLE IF NOT EXISTS technologies (
            TechName nvarchar(20),
            Comm_IN nvarchar(244),
            Comm_OUT nvarchar(244),
            Region nvarchar(20),
			CURR nvarchar(244),
			LimType nvarchar(20),
			YEAR  integer,
			TimeSlice nvarchar(20)
            ); """
    
    # create a database connection
    conn = create_connection(database)
    if conn is not None:
        # create tables
        create_table(conn, sql_FI_T_table)
    else:
        print("Error! cannot create the database connection.")
    
    
def insert2table(conn,tablename,dati,col):
    """
    Insert data in the tables
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    try:    
         if np.char.find(col,'-')!=-1:
                col= '_'.join([x for x in col.split('-')]) 
        
         place_order = ' (' + str(col) + ')'
         sql_insert= "INSERT INTO " + tablename  + place_order + " VALUES (?) "

         cur.execute(sql_insert, (dati,))
         conn.commit()
         return cur.lastrowid
     
    except sqlite3.Error as e:
         print(e)
         
         
def update2table(conn,tablename,dati,col,rowid):
    """
    Update data in the tables
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    try:    
         if np.char.find(col,'-')!=-1:
                col= '_'.join([x for x in col.split('-')]) 
         
         sql_update= 'UPDATE ' + tablename + ' SET ' + str(col)+ '=? WHERE ROWID=?'
         
         value=(str(dati),rowid)
         cur.execute(sql_update,value)
         conn.commit()
            
    except sqlite3.Error as e:
         print(e)
         
def add2table(conn,tablename,col):
    """
    ALTER column in the tables
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    try:    
         if np.char.find(col,'-')!=-1:
                col= '_'.join([x for x in col.split('-')])        
         sql_alter= 'ALTER TABLE ' + tablename + ' ADD ' + str(col) + ' integer'
         
         cur.execute(sql_alter)
         conn.commit()
         
    except sqlite3.Error as e:
         print(e)
         
         
def select2table(conn, tablename,col,rowid):
    """
    select by row id and column
    :param conn: the Connection object
    :param priority:
    :return:
    """
    cur = conn.cursor()
    sql_select='SELECT rowid,? FROM '+tablename+' WHERE ROWID=?'
    value=(str(col),rowid)
    cur.execute(sql_select,value)
    value=[]
    for data in cur.fetchall()[0]:
        value.append(data)
    return value

    
         