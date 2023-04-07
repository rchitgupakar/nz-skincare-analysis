import pandas as pd

## Author Rishab Chitgupakar Nov 24, 2022
## This is python program to combine multiple year data into a single format
## The format of the data is in Skin Cancer Excel changes based on the years


folder_csv = '/ssd/workarea/skin_cancer/data/raw_data_2023_24/extracted_data/'

# For years 2015-23, the availble columns are
# date, sc_number, name, dob, age, m_f, q, sp, clinical_indication_scc, clinical_indication_bcc, clinical_indication_mm, clinical_indication_other, 
# site, closure_type, size, scc, bcc, mm, other, code, wound_infection, prohylatic_antibiotics, type_of_referral, incomplete_excision, pigmented_lesion_benign, 
# nan_malignant, notes
filename_list = ['2015.csv', '2016.csv', '2017.csv', '2018.csv', '2019.csv', '2020.csv', '2021.csv', '2022.csv', '2023.csv']  

# Initialize an empty list to store dataframes
dfs = []

# Read each specified file into a dataframe and append it to the list
for file_name in filename_list:
    file_path = folder_csv + file_name
    df = pd.read_csv(file_path, sep='\t')
    df['file_year'] = file_name.replace('.csv','')
    dfs.append(df)

# Concatenate into one dataframe
combined_df = pd.concat(dfs, ignore_index=True)
# Only get distinct rows
distinct_df = combined_df.drop_duplicates()

# Write the combined data into the file
distinct_df.to_csv(folder_csv + 'combined_2015_2023.csv', index=False, sep='\t')
num_rows = len(distinct_df)
total_rows = len(combined_df)
print(f'Data for 2015-23, total rows : {total_rows} and unique rows : {num_rows}')


# For years 2009-2014, the availble columns are
# date, name, age, m_f, q, sp, site, flap_graft, scc, bcc, mm, other, code, wound_infection, type_of_referral, incomplete_excision
filename_list = ['2009.csv', '2010.csv', '2011.csv', '2012.csv', '2013.csv', '2014.csv']  

# Initialize an empty list to store dataframes
dfs = []

# Read each specified file into a dataframe and append it to the list
for file_name in filename_list:
    file_path = folder_csv + file_name
    df = pd.read_csv(file_path, sep='\t')
    df['file_year'] = file_name.replace('.csv','')
    dfs.append(df)

# Concatenate into one dataframe
combined_df = pd.concat(dfs, ignore_index=True)
# Only get distinct rows
distinct_df = combined_df.drop_duplicates()

# Write the combined data into the file
distinct_df.to_csv(folder_csv + 'combined_2009_2014.csv', index=False, sep='\t')
num_rows = len(distinct_df)
total_rows = len(combined_df)
print(f'Data for 2009-14, total rows : {total_rows} and unique rows : {num_rows}')


