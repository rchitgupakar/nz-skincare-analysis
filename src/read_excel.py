import pandas as pd
import re

## Author Rishab Chitgupakar Nov 8, 2022
## This is python program to load the Skin Cancer Data from Dr. Paul
## The format of the data is in Excel and we need to perform cleaning etc

## Whenever we get new data, this program needs to run to extract the data


excel_file = '/ssd/workarea/skin_cancer/data/raw_data_2023_24/SurgicalCodingNZ_Modified.xlsx'

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
	
	# Combine the first two rows to create column names E.g Clinical_Indication is on one sheet 
	# then specific SCC/BCC cancer type is on the second row
	new_column_names = df.iloc[0].astype(str) + '_' + df.iloc[1].astype(str)
	df.columns = new_column_names
	# column name replaced using above function 
	df.columns = [clean_column_name(col) for col in df.columns]
	
	# Drop the first two rows since they are now used as column names
	df = df.iloc[2:]
	
	# This is a special pandas method, example the Name and ID of the patient are empty, and per the Nurse we need to use the last row value
	# Replace empty strings in the first eight columns with the previous row's value
	# Added by Santosh Feb 3, 2023
	df.iloc[:, :8] = df.iloc[:, :8].fillna(method='ffill')
	
	# Write tab delimited file for only the sheets that start with number (example 2022, 2021) , ignore other sheets	
	csv_sheetname = clean_column_name(f'{sheet_name}')
	csv_filename = '/ssd/workarea/skin_cancer/data/raw_data_2023_24/extracted_data/' + csv_sheetname + '.csv'

	if csv_sheetname[0].isdigit():
		
		# cleaning values in the data, any check mark is considered as 'Y'
		df = df.replace('√', 'Y')
		df = df.replace('√& graft','√ & graft')
		df.to_csv(csv_filename, index=False, sep='\t')
		num_rows = len(df)

		# print(f'Data from sheet "{sheet_name}" has been extracted and saved to {csv_filename}, rows : {num_rows}')
		# Print all the column names
		column_names_str = ', '.join(df.columns)
		print(csv_sheetname + ' ' + column_names_str)
