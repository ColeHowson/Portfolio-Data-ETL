###############################################################################
#Import Libraries 
###############################################################################

import pymssql
import pandas as pd 
import numpy as np
import time 
from datetime import datetime

#Pandas Display Options 

pd.set_option('display.max_columns', None)
pd.options.mode.chained_assignment = None  # default='warn'

###############################################################################
#Import Deal Data 
###############################################################################

#Connection String Varibles 
username = 'besiuser'
pwd = 'rad!x-text-gamut-r0sen-truth-b1ack'
data_base = 'ONYXData' 
server_name = 'bes-sql'

conn = pymssql.connect(server = server_name, user = username, password = pwd, database = data_base )
cur = conn.cursor()
cur.execute('SELECT * FROM [ONYXData].[Customer].[vwDealActualEstimates]')
data = cur.fetchall()
conn.close()
column_names = [item[0] for item in cur.description]
lst = [list(row) for row in data]
df = pd.DataFrame(lst)
df.columns = column_names

###############################################################################
#Initial Cleaning 
###############################################################################

# Re-Assigning Dataframe
deal_df = df 
#Dropping Columns Not needed In Final Data 
deal_df.drop(columns = ["Id", "DealPackageId", "AccountStyle", "AssociateName","UtilityName","AccountNumber"
                             ,"PoolName","ParentPackageTransactionId", "TransportSource","FuelVolume",
                             "FuelCost", "IndexPrice","EstimatedVolume","TotalCost","LocationName","InvoiceNumber",
                             "ExcludePackageOnReporting","IntervalStart",'IntervalEnd','FiscalPeriod','StatementDate'], inplace = True )

from numpy import float64

#Correcting Data Types
deal_df['Cost'] = deal_df['Cost'].astype(float64)
deal_df['Percentage'] = deal_df['Percentage'].astype(float64)
deal_df['ConversionFactor'] = deal_df['ConversionFactor'].astype(float64)
deal_df['DealStartDate'] = pd.to_datetime(deal_df['DealStartDate'])
deal_df['DealEndDate'] = pd.to_datetime(deal_df['DealEndDate'])

###############################################################################
#Filtering Data 
###############################################################################

#Filtering Out Esimated Deals 
deal_df = df[df['ActualOrEstimate'] != 'E'] 
#Filtering Out Transport Only
deal_df = deal_df[deal_df['TemplateType'] != 'Transport Only']
#Filtering Out Balancing Gas 
deal_df = deal_df[deal_df['DealType'] != 'Balancing Gas']

###############################################################################
#Creating New Columns 
###############################################################################
from pandas import isnull

#Creating Commodity Id Column
deal_df['CommodityId'] = np.where(deal_df['Commodity'] == 'Electricity',1,2)
#Creating FixedOrIndexed Column
deal_df['FixedOrIndexed'] = np.where(deal_df['PriceType'].isnull(),'Fixed','Index')
#Dropping columns used for filtering
deal_df.drop(columns = ['ActualOrEstimate','TemplateType'], inplace = True)

###############################################################################
#Splitting The Data By Commodity 
###############################################################################

gas_df = deal_df[deal_df['Commodity'] == 'Natural Gas']
elec_df = deal_df[deal_df['Commodity'] == 'Electricity'].reset_index(drop = True)

###############################################################################
#Unit Exception Check 
###############################################################################

##### Gas Data must have a unit of either GJ or MMBTU in order for the code to continue executing #####

#Creating arrays that contain the unique values of Units
elec_unit_array = elec_df['Unit'].unique()
gas_unit_array = gas_df['Unit'].unique()


#These two lines test the arrays above to confirm they meet the required limit 
if len(elec_unit_array) > 1: 
    raise Exception('Unique Electricty Unit Limit Exceeded')

if len(gas_unit_array) > 2:
    raise Exception('Unique Natural Gas Unit Limit Exceeded')


###############################################################################
#Converting MMBTU rows to GJs
###############################################################################

#Creating a New Column with Volumes converted from MMBTU to GJs
gas_df['ConvertedVolume'] = gas_df['Volume'] * gas_df['ConversionFactor']
#Replacing MMBTU string with GJ in 'Unit' column
gas_df['Unit'] = np.where(gas_df['Unit'] == 'MMBTU','GJ','GJ')

###############################################################################
#Converting Gas Data From Transactional To Monthly 
###############################################################################

#Returning DF where each TransactionId is a unique row
gas_df = gas_df.drop_duplicates()
#Generates a DatetimeIndex of days between each deals Deal Start and End Dates
gas_df['DayOfDeal'] = gas_df.apply(lambda x: pd.date_range(start=x['DealStartDate'], end=x['DealEndDate']), axis=1)
#Creates a new row for each day in the DatetimeIndex
gas_df = gas_df.explode('DayOfDeal').reset_index(drop=True)
#Dropping time from the datetime object
gas_df['DayOfDeal'] = pd.to_datetime(gas_df['DayOfDeal']).dt.normalize()

###################################################################################
#Creating A MonthYear Column/Converting Data Back To Monthly
###################################################################################

gas_df['MonthYear'] = gas_df['DayOfDeal']

#Flooring MonthYear value to first day of month
gas_df['MonthYear'] = gas_df['MonthYear'].dt.to_period('M').dt.to_timestamp()
gas_df['MonthYear'] = gas_df['MonthYear'].astype(str)
gas_df_monthly = gas_df.groupby(['Customer','CustomerId','MonthYear','FixedOrIndexed','FiscalStart','CommodityId'])['ConvertedVolume'].sum().reset_index()


###############################################################################
#Calculating Days Per Month And DCQ
###############################################################################

def days_in_month(date):
    return pd.Period(date).days_in_month

gas_df_monthly['Days'] = gas_df_monthly['MonthYear'].apply(days_in_month)
gas_df_monthly['DailyVolume'] = gas_df_monthly['ConvertedVolume'] / gas_df_monthly['Days']
gas_df_monthly.drop(columns = ['Days','ConvertedVolume'],inplace = True)

gas_df_monthly = gas_df_monthly.pivot_table(index=['Customer', 'CustomerId','MonthYear','FiscalStart','CommodityId'], columns='FixedOrIndexed',values='DailyVolume').reset_index()

###############################################################################
#Importing And Cleaning Pool and DCQ tables 
###############################################################################

#Importing DCQ Data 
conn = pymssql.connect(server = server_name, user = username, password = pwd, database = data_base)
cur = conn.cursor()
cur.execute('select * from [ONYXData].[NaturalGas].[DCQ]')
data = cur.fetchall()
conn.close()
column_names = [item[0] for item in cur.description]
lst = [list(row) for row in data]
dcq_df = pd.DataFrame(lst)
dcq_df.columns = column_names

#Importing Pool Data 
conn = pymssql.connect(server = server_name, user = username, password = pwd, database = data_base)
cur = conn.cursor()
cur.execute('select * from [ONYXData].[Customer].[OrganizationAccount] where IsPool = 1')
data = cur.fetchall()
conn.close()
column_names = [item[0] for item in cur.description]
lst = [list(row) for row in data]
pool_df = pd.DataFrame(lst)
pool_df.columns = column_names

#Renaming pool_df columns and merging with DCQ dataframe 
pool_df = pool_df.rename(columns = {'Id':'PoolId','AccountNo':'PoolName'})
dcq_data = pd.merge(dcq_df,pool_df, how = "inner", on = 'PoolId' )
dcq_data = dcq_data[['CustomerId','PoolName','CommencementDate','TerminationDate','Volume']]

#Converting the Data from Monthly to Daily 
dcq_data['DayOfDeal'] = dcq_data.apply(lambda x: pd.date_range(start=x['CommencementDate'], end=x['TerminationDate']), axis=1)
dcq_data = dcq_data.explode('DayOfDeal').reset_index(drop=True)
dcq_data['DayOfDeal'] = pd.to_datetime(dcq_data['DayOfDeal']).dt.normalize()

dcq_data['MonthYear'] = dcq_data['DayOfDeal']
dcq_data['MonthYear'] = dcq_data['MonthYear'].dt.to_period('M').dt.to_timestamp()
dcq_data['MonthYear'] = dcq_data['MonthYear'].astype(str)
dcq_data.drop(columns = "PoolName",inplace = True)

dcq_data_monthly = dcq_data.groupby(['CustomerId','MonthYear']).sum('Volume').reset_index()

#Calculating DCQ
dcq_data_monthly['TotalDCQ'] = dcq_data_monthly['Volume']
dcq_data_monthly['Days'] = dcq_data_monthly['MonthYear'].apply(days_in_month)
dcq_data_monthly['DCQ'] = dcq_data_monthly['TotalDCQ'] / dcq_data_monthly['Days']
dcq_data_monthly.drop(columns = ['Volume','Days'], inplace = True )

###############################################################################
#Merging The Two Gas Tables 
###############################################################################

#Merging Transaction and DCQ tables 
gas_df = pd.merge(gas_df_monthly,dcq_data_monthly, how = 'inner', on = ['CustomerId','MonthYear'])
gas_df = gas_df.fillna(0)
gas_df['Fixed'] = gas_df['Fixed'].astype(int).round(decimals= 0)
gas_df['Index'] = gas_df['Index'].astype(int).round(decimals= 0)
gas_df['DCQ'] = gas_df['DCQ'].astype(int).round(decimals= 0)

#Creating "Remaining Column"
gas_df['Remaining'] = np.where(gas_df['DCQ'] - (gas_df['Fixed'] + gas_df['Index']) < 0, 0, gas_df['DCQ'] - (gas_df['Fixed'] + gas_df['Index']))

#Re-ordering table columns 
gas_df = gas_df[['CustomerId','Customer','MonthYear','DCQ','Fixed','Index','Remaining','CommodityId','FiscalStart']]
gas_df = gas_df.rename(columns={'MonthYear':"TransactionMonth"}).reset_index()

###############################################################################
###############################################################################
#ELECTRICITY 
###############################################################################
###############################################################################

#Converting Monthly Electricty Transactions to Daily
elec_df = elec_df.drop_duplicates()
elec_df['DayOfDeal'] = elec_df.apply(lambda x: pd.date_range(start=x['DealStartDate'], end=x['DealEndDate']), axis=1)
elec_df = elec_df.explode('DayOfDeal').reset_index(drop=True)
elec_df['DayOfDeal'] = pd.to_datetime(elec_df['DayOfDeal']).dt.normalize()

#Creating 'MonthYear Column
elec_df['MonthYear'] = elec_df['DayOfDeal']
elec_df['MonthYear'] = elec_df['MonthYear'].dt.to_period('M').dt.to_timestamp()
elec_df['MonthYear'] = elec_df['MonthYear'].astype(str)

#Converting Daily Data Back to Monthly 
elec_df_monthly = elec_df.groupby(['Customer','CustomerId','MonthYear','FixedOrIndexed','FiscalStart','CommodityId',"Percentage"])['Volume'].sum().reset_index()

#Calculating Daily Volume/Creating Seperate Columns for Fixed and Index 
elec_df_monthly['Days'] = elec_df_monthly['MonthYear'].apply(days_in_month)
elec_df_monthly['DailyVolume'] = elec_df_monthly['Volume'] / elec_df_monthly['Days']
elec_df_monthly.drop(columns= ['Days'],inplace = True)

elec_df_monthly['Percentage'] = np.where(elec_df_monthly['Percentage'] == 50.0, 0.5, 1 )
elec_df_monthly['ConvertedDailyVolume'] = elec_df_monthly['DailyVolume'] * elec_df_monthly['Percentage']
elec_df_monthly = elec_df_monthly.drop(columns = 'DailyVolume')

elec_df_monthly = elec_df_monthly.pivot_table(index=['Customer', 'CustomerId','MonthYear','FiscalStart','CommodityId','Percentage'], columns='FixedOrIndexed',values='ConvertedDailyVolume').reset_index()


#Fixing Data Types,Renaming and Re-ordering the DF 
elec_df_monthly= elec_df_monthly.fillna(0)
elec_df_monthly['Fixed'] = elec_df_monthly['Fixed'].astype(int).round(decimals= 0)
elec_df_monthly['Index'] = elec_df_monthly['Index'].astype(int).round(decimals= 0)
elec_df= elec_df_monthly.rename(columns = {'MonthYear':'TransactionMonth'})
elec_df = elec_df[['CustomerId','Customer','TransactionMonth','Fixed','Index','CommodityId','FiscalStart']].reset_index(drop = True)

###############################################################################
#Final Data 
###############################################################################

final_df = pd.concat([elec_df,gas_df],axis = 0, ignore_index= True)
final_df = final_df.drop(columns = 'index')
final_df = final_df.reset_index(drop = True)

final_df = final_df[['CustomerId','Customer','TransactionMonth','DCQ','Fixed','Index','Remaining','CommodityId','FiscalStart']]
final_df= final_df.fillna(0)

final_df['DCQ'] = final_df['DCQ'].astype(int).round(decimals= 0)
final_df['Remaining'] = final_df['Remaining'].astype(int).round(decimals= 0)
final_df['TransactionMonth'] = pd.to_datetime(final_df['TransactionMonth'])

final_df = final_df.rename(columns={'TransactionMonth': 'TransactionDate', 'Index': 'Indexed'})

cols = ['Customer', 'CustomerId', 'TransactionDate', 'DCQ', 'Fixed', 'Indexed', 'Remaining', 'CommodityId', 'FiscalStart']
final_df = final_df[cols]




###############################################################################
#Pushing Data to SQL Database
###############################################################################

username = 'besiuser'
pwd = 'rad!x-text-gamut-r0sen-truth-b1ack'
data_base = 'ONYXData' 
server_name = 'bes-sql'


#Delete Current Data from Table 
conn = pymssql.connect(server = server_name, user = username, password = pwd , database = data_base)
cur = conn.cursor()
cur.execute("DELETE FROM [ONYXData].[pbi].[PortfolioMonthlyDCQ]")
conn.commit()
 
print("Data Cleared")

#Insert New Data Into Table 
query = "INSERT INTO [ONYXData].[pbi].[PortfolioMonthlyDCQ] values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
sql_data = tuple(map(tuple,final_df.values))
cur.executemany(query,sql_data)
conn.commit()
cur.close()
conn.close()

print("Complete")
##check = final_df[(final_df['CommodityId']== 1) & (final_df['Fixed'] != 0)]

#check.to_csv(r"C:\Users\chowson\Desktop\check.csv")

