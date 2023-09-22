import sqlite3
import pandas as pd
from pydantic import BaseModel
from conversion import Conversion,TYPES
import names
import random
import uuid

class Connection:
    
    def __init__(self,database:str) -> None:
        self.con = sqlite3.connect(database)

    def selectDatabase(self,database:str) -> bool:
        self.con.close()
        self.con = sqlite3.connect(database)
        return True
    
    def createSqlTable(self,name:str,table:BaseModel) -> BaseModel:

        columns =", ".join([f"{c} {TYPES.get(v.annotation.__name__)} {v.default}" for c,v in table.model_fields.items()])

        SQL_STATEMENT = f"""CREATE TABLE {name} ( {columns} )"""

        self.con.execute(SQL_STATEMENT)

        return f"columns: {', '.join(table.model_fields.keys())}\ntypes: {', '.join([x.annotation.__name__ for x in table.model_fields.values()])}"
    
    def getRecord(self,table:str,columns:list="*",args:str="") -> pd.DataFrame:
        
        SQL_STATEMENT = f"""SELECT {columns} FROM {table} {args}""" # sql query for getting the data

        sql_data = pd.read_sql(sql=SQL_STATEMENT,con=self.con) # sql data converted into dataframe
        
        if sql_data.index.size == 1: # single record
            return AppSystem(**sql_data.to_dict('records')[0])
        else:
            return sql_data # multiple records


    def insertRecord(self,table:str,data:BaseModel|list[BaseModel]|pd.DataFrame,args:str="") -> str:
        
        if isinstance(data,BaseModel):
            sql_column = data.model_fields.keys()
            sql_data = [f"'{getattr(data,x)}'" for x in sql_column]
            SQL_STATEMENT = f"""INSERT INTO {table} ({", ".join(sql_column)}) VALUES ({", ".join(sql_data)}) {args}"""
            self.con.execute(SQL_STATEMENT)
            self.con.commit()
            return True
        
        if isinstance(data,list):
            for row in data:
                sql_column = row.model_fields.keys()
                sql_data = [f"'{getattr(row,x)}'" for x in sql_column]
                SQL_STATEMENT = f"""INSERT INTO {table} ({", ".join(sql_column)}) VALUES ({", ".join(sql_data)}) {args}"""
                self.con.execute(SQL_STATEMENT)
                self.con.commit()
            return True
        
        if isinstance(data,pd.DataFrame):
            data.to_sql(name=table,con=self.con,if_exists='append')
            return True

        return
    
    def updateRecord(self,table:str,args:str="") -> pd.DataFrame:
        return

    def deleteRecord(self,table:str,args:str="") -> bool:
        return
    
    def readQuery(self,table:str,query:str) -> bool | pd.DataFrame:
        return
    
    def readSql(self,table:str,sql:str) -> bool | pd.DataFrame:
        return
    
    def readTable(self,table:str) -> pd.DataFrame:
        return

    def __del__(self) -> None:
        self.con.close()

class AppSystem(BaseModel):
    id:str=""
    full_name:str=""
    first_name:str=""
    last_name:str=""
    email:str=""
    role:str=""
    password:str=""