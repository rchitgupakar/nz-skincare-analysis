import pandas as pd
import re

## Author Rishab Chitgupakar Nov 8, 2022
## This is python program to load the Skin Cancer Data from Dr. Paul
## The format of the data is in Excel and we need to perform cleaning etc

## Whenever we get new data, this program needs to run to extract the data


excel_file = '/ssd/workarea/skin_cancer/data/raw_data_2023_24/Address_List.xlsx'

# Reading the excel file using pandas
xls = pd.ExcelFile(excel_file)

# We need to clean the column names, the names have various characters like '/','\','+' etc
# We need to convert them to lower case as well
def clean_column_name(column_name):
	# Using the regular expressions to replace non-alpha characters with _
	clean_name = re.sub(r'\_nan', '', column_name.strip())
	clean_name = re.sub(r'[^a-zA-Z0-9]+', '_', clean_name.lower())
	clean_name = clean_name.replace('nan_bcc','clinical_indication_bcc')
	clean_name = clean_name.replace('nan_mm','clinical_indication_mm')
	clean_name = clean_name.replace('nan_other','clinical_indication_other')
	clean_name = clean_name.replace('nan_malignant','malignant')
	return clean_name

# There are sheets for each year raning from 2023 to 2008  
# Need to extract data from each sheet. From year 2014, the format of the data has changed
for sheet_name in xls.sheet_names:
	# Read the sheet into a DataFrame
	df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
	
	# Combine the first row to create column names
	new_column_names = df.iloc[0].astype(str) 
	df.columns = new_column_names
	# column name replaced using above function 
	df.columns = [clean_column_name(col) for col in df.columns]
	
	# Drop the first row since they are now used as column names
	df = df.iloc[1:]
	
	# Write tab delimited file for only the sheets that start with number (example 2022, 2021) , ignore other sheets	
	csv_sheetname = clean_column_name(f'{sheet_name}')
	csv_filename = '/ssd/workarea/skin_cancer/data/raw_data_2023_24/extracted_data/' + csv_sheetname + '.csv'

	df.to_csv(csv_filename, index=False, sep='\t')
	num_rows = len(df)

	print(f'Data from sheet "{sheet_name}" has been extracted and saved to {csv_filename}, rows : {num_rows}')
	
