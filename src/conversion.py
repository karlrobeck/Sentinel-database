from pydantic import BaseModel
import pandas as pd

TYPES = {
    'int': 'INT',
    'float': 'FLOAT',
    'str': 'TEXT',
    'bool': 'BIT',
    'list': 'BLOB',
    'dict': 'BLOB',
}

class Conversion:
    
    @staticmethod
    def toDataframe(self) -> pd.DataFrame:
        return
    
    @staticmethod
    def toSql(self) -> str:
        return
    
    @staticmethod
    def toPydantic(self) -> BaseModel:
        return 