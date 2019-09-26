#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 16:28:52 2019

@author: Matteo D'Andrea (s180192)
"""
###################### LYBRARIES AND PACKAGES #################################
import os
import pandas as pd
import numpy as np
from TableimportF import tableImport

########################## USER INPUTS ########################################
# define the folder from where the files to be imported are located
foldername='times-dk'



######################## DIRECTORY SETTINGS ##################################

# the VT files,system setting and BY_trans are selected
filelist=[]
for subdir, dirs, files in os.walk('./'+foldername):
    for file in files:
        if file.startswith('VT') or file in ['SysSettings.xlsx','BY_Trans']:
            filelist.append(file)

# the working directory is set to the folder selected
cwd = os.getcwd()
os.chdir(cwd+'/'+foldername)

################# FUNCTION OUTPUT EXTRACTION #################################


# a dictionary is created to store the tables 
dataframe_collector={}

# the dictionary is filled with the tables from each filename
# tableImport is the function that extracts the tables from the whole file excel
for filename in filelist:
    dataframe_collector.update(tableImport(filename))

################ FILTERING OF DATA PER CATEGORY ##############################

items=[]
basket=[]



# the VT files are divided in commodities,processes and technologies

for j in ['FI_Comm','FI_Process','FI_T']:
    
    # from the dictionary the tables of each category are taken  
    dict_keys=list(dataframe_collector.keys())
    selDf=np.char.find(dict_keys,j)!=-1
    c=pd.Series(dict_keys).loc[selDf==True]

    
    # the default units are extracted from the sysSettings file
    selDf=np.char.find(dict_keys,'DefUnits')!=-1
    DefUnits=pd.Series(dict_keys).loc[selDf==True].tolist()[0]
    dataframe_collector[DefUnits].set_index(\
                       dataframe_collector[DefUnits].iloc[:,0],inplace=True)
    
    # a dataframe stores the joint tables 
    df=pd.DataFrame([])
    
    # if the unit is not defined by the user the default unit will be applied
    # if the default unit is not found in syssetting file a message is printed 
    for i in c: 
        try :
            if i.split('~')[1] == 'FI_Comm':
                dataframe_collector[i].loc[:,'Unit'].mask(\
                                dataframe_collector[i].loc[:,'Unit']=='nan',\
                                dataframe_collector[DefUnits].loc['Process_ActUnit',\
                                                    (i.split('-')[1].split('_')[2])])
            elif i.split('~')[1] == 'FI_Process':
                for x,z in ['Process_ActUnit','Tact'],['Process_CapUnit','Tcap']:
                    dataframe_collector[i].loc[:,z].mask(\
                                   dataframe_collector[i].loc[:,z]=='nan',\
                                   dataframe_collector[DefUnits].loc[x,\
                                                     (i.split('-')[1].split('_')[2])])
        except KeyError:
            print(\
        ('the default unit for {} is not defined in the SysSettings file'\
         ).format(i.split('-')[1]))
        
        # the tables selected are concatenated together
        df=df.append(dataframe_collector[i],ignore_index = True,sort=False)
            
    ################ IDENTIFY THE COLUMN ######################################
    
    Regioncol=[col for col in df.columns if 'region' in col.lower()][0]
    Setscol=[col for col in df.columns if 'set' in col.lower()]
    Namecol=[col for col in df.columns if 'name' in col.lower()][0]
    ######################### ERROR CHECK #################################
    
    # check for commodities/process/technology not defined 
    if np.any(df[Namecol].values =='nan'):
        raise ValueError ('A commodity/process/technology name is missing')
   
    #check for commodities defined more than once in the same region
    names=(y for y in df[Namecol].unique() if y not in ['None', '', None, 'nan'])
    for item in names:
            cond1=df[Namecol]==item
            if df[Namecol].loc[cond1].shape[0]>1:
                for region in df[Regioncol].loc[cond1].values:
                    cond2=df[Regioncol]==region
                    if df[Regioncol].loc[cond1 & cond2].shape[0]>1:
                        print(('In {} > {} : {} is defined more than once').format(\
                          j,Namecol,item))
                
    ################# FILL THE TABLE #############################################
    # the sets for commodities and processes are set 
    if j in ['FI_Comm','FI_Process']:
        for i in range(df.shape[0]):
            if df.loc[i,Setscol][0] == 'None' :
              df.loc[i,Setscol] =  df.loc[i-1,Setscol]
              
    ################## SAVE THE OUTPUT ##########################################
    #the final dataframe is assigned to a category
    
    df.to_excel(j+'.xlsx',index=None,header=True, float_format="%,2f")
    
    df.replace(['None','',float('NaN')], np.nan,inplace=True)
    
#    if j == 'FI_T':
#            sortedcol=[*df.columns[:i[4]+1],*sorted(df.columns[i[4]+1:])]
#            df=df.reindex(sortedcol, axis=1)
            
    if j == 'FI_Comm': 
        commodities=df
    elif j == 'FI_Process':
         processes=df
    else:
        technologies=df
        
#################### VALUE CHECK on joint tables ############################### 
        
# check for processes referenced in the technology table 
# but not defined in the process table 
for item in technologies['TechName'].values.tolist(): 
    if item!= 'None' :
        if item  in processes['TechName']:
            raise ValueError ('Undefined process referenced in the technology table')
 

# check for commodities used in the technology 
for name in ['Comm-IN','Comm-OUT','Comm-IN-A','Comm-OUT-A']:
    items=[*items, *technologies[name].dropna().unique().tolist()]
 
# multiple input/output are separated and appended to the list if not present 
for i in items:
    if np.char.find(i,',')!=-1:
        for k in i.split(','):
            if k not in items:
                items.append(k)
                
# the list is cleared from unwanted characters
items[:]= [x for x in items if np.char.find(x,',')==-1 and \
                     x not in ['None', '', None, 'nan']]

# if the commodity is not defined an error is raised 
for item in items:
    if item not in commodities['CommName'].tolist():
        raise ValueError ('Undefined commodity referenced in the technology table') 
      
        
################# INDEXES DEFINITION #########################################      

#extract regions from system settings
selDf=np.char.find(dict_keys,'BookRegions_Map')!=-1
reg_def=pd.Series(dict_keys).loc[selDf==True].tolist()[0]
regions= dataframe_collector[reg_def].loc[:,'Region'].values.tolist()


#extract starting year from system settings
selDf=np.char.find(dict_keys,'~StartYear')!=-1
year_def=pd.Series(dict_keys).loc[selDf==True].tolist()[0]
start_year= dataframe_collector[year_def].columns.tolist()[0]


index_dic={}

for r in regions:
    index_dic[r]='Region'
for y in start_year:
    index_dic[y]='YEAR' 
for w in ['UP','LO']:
    index_dic[y]='LimType'
    
index_dic[int]='YEAR'

list_indexes=['TechName','Comm-IN','Comm-OUT','CURR','LimType',\
              'Region','YEAR','TimeSlice']

    
######################### SQL Connection ######################################
        
#change directory to the parent folder
os.chdir('..')

#select database connected 
database = "input_data_SQLite.db"

from SQlite_database import insert_into_tables, main_sql, create_connection

#establish connection 
conn=create_connection(database)

#call main function to create table 
main_sql(database)

#populate the database
for tablename,data in ['commodities',commodities],['processes', processes]:
    
    insert= "INSERT INTO " + tablename + " VALUES ("
    place_order = ' ,'.join(['?' for row in data.columns]) 
    sql_insert= insert + place_order + ')'
    for i in range(data.shape[0]):
        insert_into_tables(conn,sql_insert,data.iloc[i,:].values.tolist())
        conn.commit()
       
           

