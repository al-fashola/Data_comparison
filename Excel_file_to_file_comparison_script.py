#importing libraries
import pandas as pd
import datacompy
#import numpy as np
from openpyxl import load_workbook
#import datetime as dt
#from pathlib import Path
import sys


#fetch the input file
df_file_1 = pd.read_excel("//Users/.xlsx")
df_file_2 =pd.read_excel("//Users/.xlsx")


print(df_file_1.head(5))
df_file_2.head(5)

df_file_1=df_file_1.astype(str)
df_file_2=df_file_2.astype(str)

#comapring the dataframe
compare = datacompy.Compare(df_file_1,df_file_2,join_columns=['Id'])
print(compare.report())
with open('file_to_file_comparison_report_1.txt', 'w',encoding='utf-8') as f:
    f.write(compare.report())
