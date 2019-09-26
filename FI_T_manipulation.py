#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 14:31:19 2019

@author: matteo
"""
###################### LYBRARIES AND PACKAGES #################################
import pandas as pd

import numpy as np 

from SQlite_database import main_sql_FI_T,create_connection,\
                            insert2table,update2table,add2table,select2table

########################## USER INPUTS ########################################

#table to test the algorithm 
file='test-data.xlsx'
#archive with attribute default data
AttMaster='AttributeMaster.xlsx'

#select database and table 
database='FI_T_SQLite2.db'
tablename='technologies'

######################## DATA LOADING ########################################
dati=pd.read_excel(file)
AttMaster= pd.read_excel(AttMaster,na_values='')

###################### DATAFRAME MANIPULATION ################################

#unwanted columns are discarded
selrow=np.char.find(dati.iloc[:,0].tolist(),'*')==-1
dati=dati[selrow]
selcol=np.char.find(dati.columns.tolist(),'*')==-1
dati=dati[dati.columns.values[selcol]]
dati.reset_index(drop=True,inplace=True)

# a list of attribute is created
att_list=AttMaster.loc[:,'Attribute'].values.tolist()

for i in AttMaster.loc[:,'Alias'].values.tolist():
    if type(i)!= float  and np.char.find(i,'/')!=-1:
        for x in i.split('/'):
            att_list.append(x)
    elif type(i)!= float:
        att_list.append(i) 


################# INDEXES DEFINITION #########################################

#the indexes are defined
list_indexes=['TechName','Comm-IN','Comm-OUT','CURR','LimType',\
              'Region','YEAR','TimeSlice']



#the dictionary to select index column is generated
index_dic={}
regions=['DKE','DKW']

for r in regions:
    index_dic[r]='Region'
#for y in start_year:
#    index_dic[y]='YEAR' 
for w in ['UP','LO','FX']:
    index_dic[w]='LimType'
    
#the starting year is defined 
start_year=2010

################## DATABASE CONNECTION #######################################

#establish connection 
conn=create_connection(database)

#call main function to create table 
main_sql_FI_T(database)
#########################  POPULATE THE DATABASE ##########################

for x in range(dati.shape[0]):
    for head in dati.columns:
        if head in list_indexes:
            #the techname must be defined on a new row
            if head == 'TechName':
                rowid=insert2table(conn,tablename,dati.loc[x,head],head)
            else:
                #the data are inserted on the same row as techname
                update2table(conn,tablename,dati.loc[x,head],head,rowid)
        else:
            #the attribute are analysed with the index specific values
            if np.char.find(head,'~')!=-1:
                for i in head.split('~'):
                    if i in att_list:
                        #the attribute column is stored and added to the db
                        good_col=i
                        add2table(conn,tablename,i)
                    #the years are inserted in the year column 
                    elif type(i) is int and i >= start_year :
                        cell_value=select2table(conn,tablename,'YEAR',rowid)[1]
                        cell_row=select2table(conn,tablename,'YEAR',rowid)[0]
                        #a multindex structure is created
                        if  cell_value == 'nan':
                            update2table(conn,tablename,start_year,'YEAR',cell_row)
                            update2table(conn,tablename,dati.loc[x,head],good_col,cell_row)
                        elif cell_value != i:
                            insert2table(conn,tablename,i,'YEAR')
                            update2table(conn,tablename,dati.loc[x,head],good_col,cell_row+1)
                        else:
                            update2table(conn,tablename,dati.loc[x,head],good_col,cell_row)
                    #other index specific values (i.e. LimType,Region) are considered
                    elif type(i) is str:
                        #from the dictionary the specific index is found
                        sel_col=index_dic[i]
                        #the cell value and row position are found
                        cell_value=select2table(conn,tablename,sel_col,rowid)[1]
                        cell_row=select2table(conn,tablename,sel_col,rowid)[0]
                        #a multindex structure is created
                        if  cell_value == 'nan':
                            update2table(conn,tablename,start_year,sel_col,cell_row)
                            update2table(conn,tablename,dati.loc[x,head],good_col,cell_row)
                        elif cell_value != i:
                            insert2table(conn,tablename,i,sel_col)
                            update2table(conn,tablename,dati.loc[x,head],good_col,cell_row+1)
                        else:
                            update2table(conn,tablename,dati.loc[x,head],good_col,cell_row)
                        
                        
                        
                