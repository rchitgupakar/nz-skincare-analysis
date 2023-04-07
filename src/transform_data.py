import pandas as pd
import json

## Author Rishab Chitgupakar Jan 11, 2023
## This is python program using Apache Spark to derive fields



folder_csv = '/ssd/workarea/skin_cancer/data/raw_data_2023_24/extracted_data/'

from pyspark.sql import SparkSession

# start a SparkSession
spark = SparkSession.builder.appName("SkinCancerInsights") \
        .config("spark.jars", "/ssd/workarea/skin_cancer/code/postgresql-42.3.4.jar").getOrCreate()

# added the postgresql per instruction of Santosh

# Reading the data from 2015_2023
file_path = folder_csv + 'combined_2015_2023.csv'
df_2015_2023 = spark.read.csv(file_path, sep='\t', header=True, inferSchema=True)
# create a temporary view so that we can use it on Spark SQL
df_2015_2023.createOrReplaceTempView("table_2015_2023")


# Reading the data from 2009_2014
file_path = folder_csv + 'combined_2009_2014.csv'
df_2009_2014 = spark.read.csv(file_path, sep='\t', header=True, inferSchema=True)
# create a temporary view so that we can use it on Spark SQL
df_2009_2014.createOrReplaceTempView("table_2009_2014")

# Reading the data from 2008
file_path = folder_csv + '2008.csv'
df_2008 = spark.read.csv(file_path, sep='\t', header=True, inferSchema=True)
# create a temporary view so that we can use it on Spark SQL
df_2008.createOrReplaceTempView("table_2008")


# Combine the data to get common attributes across all years. The formats changed in 2009 and 2014
combined_data = spark.sql("""
SELECT file_year, date as date_of_treatment, name, dob, age, m_f as gender, q as quintile, site, closure_type, scc, bcc, mm, other, wound_infection, prohylatic_antibiotics, type_of_referral, incomplete_excision
 FROM table_2015_2023 
union 
SELECT file_year, date as date_of_treatment, name, cast(null as string) as dob, age, m_f as gender, q as quintile, site, flap_graft as closure_type, scc, bcc, mm, other,  wound_infection, 
cast(null as string) as prohylatic_antibiotics, type_of_referral, incomplete_excision FROM table_2009_2014 
union 
SELECT 2008 as file_year, date as date_of_treatment, name, cast(null as string) as dob, age, m_f as gender, q as quintile, site, flap_graft as closure_type, scc, bcc, mm, other,  wound_infection, 
cast(null as string) as prohylatic_antibiotics, type_of_referral, incomplete_excision FROM table_2008  """)


# combined_data.show()

# print(df_2015_2023.count() , ' ' , combined_data.count())

combined_data.createOrReplaceTempView("combined_data")

# Transform the data to derive some attributes (This came with analysis of data, and talks with Dr Paul)
# scc_flag, bcc_flag, mm_flag, cancer_flag, sub_type discussion done with Dr Paul and Santosh on 13/12/2022
# site of the cancer added on 30/01/2023 per instruction Dr Paul. The incorrect data was corrected
# added filters so that incorrect rows are not taken 29/01/2023

combined_data2 = spark.sql("""
select file_year, lower(date_of_treatment) as date_of_treatment, lower(name) as name, lower(dob) as dob, cast(age as int) as age, lower(other) as other,
case when trim(lower(gender)) = 'm' then 'Male'
         when trim(lower(gender)) = 'f' then 'Female' else 'N/A' end as gender,
case when (quintile = '1' or quintile = '2' or quintile = '3' or quintile = '4' or quintile = '5' ) then quintile else 'Not Provided' end as quintile,
case when scc in ('1','Y','SCC','1.0') then 'Squamous Cell Carcinoma'  else 'N/A' end as scc_flag, 
case when bcc in ('1','Y','BCC','1.0') then 'Basal Cell Carcinoma' else 'N/A' end as bcc_flag, 
case when mm in ('1','Y','MM','1.0') then 'Melanoma' else 'N/A' end as mm_flag, 
case when scc in ('1','Y','SCC','1.0') and lower(other) rlike 'Well' then 'Well_differentiated-SCC'
     when scc in ('1','Y','SCC','1.0') and lower(other) rlike 'Mod' then 'Moderately_differentiated-SCC'
     when scc in ('1','Y','SCC','1.0') and lower(other) rlike 'poor' then 'Poorly_differentiated-SCC'
     when scc in ('1','Y','SCC','1.0') and lower(other) rlike  'Kerato' and lower(other) not rlike 'Well' then 'Low_grade-SCC'
     when scc in ('1','Y','SCC','1.0') and lower(other) not rlike 'Well' and lower(other) not rlike 'Mod' and lower(other) not rlike 'poor' and lower(other) not rlike 'Kerato' then 'SCC'
     when bcc in ('1','Y','BCC','1.0') and lower(other) rlike  'Nodular' and lower(other) not rlike 'micro' then 'Nodular-BCC'
     when bcc in ('1','Y','BCC','1.0') and lower(other) rlike  'Infil' and lower(other) not rlike 'Nodular' then 'Infiltrating-BCC'
     when bcc in ('1','Y','BCC','1.0') and lower(other) rlike  'micro' then 'Micronodular-BCC'
     when bcc in ('1','Y','BCC','1.0') and lower(other) rlike  'sclerosing' or lower(other) rlike  'morpho' then 'Sclerosing-BCC'
     when bcc in ('1','Y','BCC','1.0') and lower(other) not rlike 'sclerosing' and lower(other) not rlike 'micro' and lower(other) not rlike 'Infil' and lower(other) not rlike 'Nodular' then 'BCC'
     when mm in ('1','Y','MM','1.0') and lower(other) rlike  'situ' then 'Melanoma In Situ-MM'
     when mm in ('1','Y','MM','1.0') and lower(other) rlike  'melanoma' and lower(other) not rlike 'situ' then 'Melanoma_invasive_MM'
     when mm in ('1','Y','MM','1.0') and lower(other) not rlike 'melanoma' and lower(other) not rlike 'situ' then 'MM' else cast(null as string) end as sub_type, 
case when scc in ('1','Y','SCC','1.0') then 1 
 when bcc in ('1','Y','BCC','1.0') then 1
 when mm in ('1','Y','MM','1.0') then 1 else 0 end as cancer_flag,
case when derived_site = 'ace' then 'face'
when derived_site = 'back' then 'back'
when derived_site = 'biopsy' then 'biopsy'
when derived_site = 'caudal' then 'caudal'
when derived_site = 'chest' then 'trunk'
when derived_site = 'chin' then 'chin'
when derived_site = 'cranial' then 'cranial'
when derived_site = 'ear' then 'ear'
when derived_site = 'eye' then 'eye'
when derived_site = 'eyelid' then 'eyelid'
when derived_site = 'eylid' then 'eyelid'
when derived_site = 'faace' then 'face'
when derived_site = 'facd' then 'face'
when derived_site = 'facde' then 'face'
when derived_site = 'face' then 'face'
when derived_site = 'faceeye' then 'eye'
when derived_site = 'fae' then 'face'
when derived_site = 'faec' then 'face'
when derived_site = 'fave' then 'face'
when derived_site = 'faxce' then 'face'
when derived_site = 'fcace' then 'face'
when derived_site = 'fcae' then 'face'
when derived_site = 'fce' then 'face'
when derived_site = 'finger' then 'finger'
when derived_site = 'flap' then 'trunk'
when derived_site = 'foot' then 'foot'
when derived_site = 'footd' then 'foot'
when derived_site = 'frace' then 'face'
when derived_site = 'ftrunk' then 'trunk'
when derived_site = 'fulimb' then 'upper limb'
when derived_site = 'groin' then 'groin'
when derived_site = 'hand' then 'hand'
when derived_site = 'limb' then 'upper limb'
when derived_site = 'lip' then 'lip'
when derived_site = 'lipface' then 'lip'
when derived_site = 'lleg' then 'lower limb'
when derived_site = 'llib' then 'lower limb'
when derived_site = 'llimb' then 'lower limb'
when derived_site = 'llimbe' then 'lower limb'
when derived_site = 'llimbn' then 'lower limb'
when derived_site = 'llimbs' then 'lower limb'
when derived_site = 'llimg' then 'lower limb'
when derived_site = 'llimp' then 'lower limb'
when derived_site = 'llimv' then 'lower limb'
when derived_site = 'lllimb' then 'lower limb'
when derived_site = 'llmb' then 'lower limb'
when derived_site = 'llmib' then 'lower limb'
when derived_site = 'llomb' then 'lower limb'
when derived_site = 'llumb' then 'lower limb'
when derived_site = 'lower' then 'lower limb'
when derived_site = 'lowerl' then 'lower limb'
when derived_site = 'lowerlimb' then 'lower limb'
when derived_site = 'lowerllimb' then 'lower limb'
when derived_site = 'lshoulder' then 'trunk'
when derived_site = 'lthigh' then 'thigh'
when derived_site = 'lup' then 'lip'
when derived_site = 'mouth' then 'lip'
when derived_site = 'neck' then 'neck'
when derived_site = 'necl' then 'neck'
when derived_site = 'nekc' then 'neck'
when derived_site = 'nise' then 'nose'
when derived_site = 'nose' then 'nose'
when derived_site = 'ose' then 'nose'
when derived_site = 'rshoulder' then 'trunk'
when derived_site = 'runk' then 'trunk'
when derived_site = 'sca' then 'scalp'
when derived_site = 'scako' then 'scalp'
when derived_site = 'scalp' then 'scalp'
when derived_site = 'scap' then 'scalp'
when derived_site = 'sclap' then 'scalp'
when derived_site = 'shave' then 'shave'
when derived_site = 'temple' then 'face'
when derived_site = 'thrunk' then 'trunk'
when derived_site = 'tongue' then 'tongue'
when derived_site = 'tounge' then 'tongue'
when derived_site = 'trnk' then 'trunk'
when derived_site = 'truck' then 'trunk'
when derived_site = 'trudnk' then 'trunk'
when derived_site = 'truk' then 'trunk'
when derived_site = 'trunck' then 'trunk'
when derived_site = 'trunk' then 'trunk'
when derived_site = 'trunkj' then 'trunk'
when derived_site = 'trunl' then 'trunk'
when derived_site = 'tunk' then 'trunk'
when derived_site = 'turunk' then 'trunk'
when derived_site = 'ulib' then 'upper limb'
when derived_site = 'ulim' then 'upper limb'
when derived_site = 'ulimb' then 'upper limb'
when derived_site = 'ulimbs' then 'upper limb'
when derived_site = 'ulimv' then 'upper limb'
when derived_site = 'ulinb' then 'upper limb'
when derived_site = 'ullimb' then 'upper limb'
when derived_site = 'ulmb' then 'upper limb'
when derived_site = 'ulumb' then 'upper limb'
when derived_site = 'uper' then 'upper limb'
when derived_site = 'upperlimb' then 'upper limb'
when derived_site = 'upperlimbs' then 'upper limb'
when derived_site = 'upperlmb' then 'upper limb'
when derived_site = 'vas' then 'vas'
else 'N/A'
end as site ,

case when dclosure_type = 'biopst' then 'biospy'
when dclosure_type = 'biopsu' then 'biospy'
when dclosure_type = 'biopsuy' then 'biospy'
when dclosure_type = 'biopsy' then 'biospy'
when dclosure_type = 'biosp' then 'biospy'
when dclosure_type = 'biospy' then 'biospy'
when dclosure_type = 'biosy' then 'biospy'
when dclosure_type = 'biposy' then 'biospy'
when dclosure_type = 'bopsy' then 'biospy'
when dclosure_type = 'falp' then 'flap'
when dclosure_type = 'fla' then 'flap'
when dclosure_type = 'flao' then 'flap'
when dclosure_type = 'flap' then 'flap'
when dclosure_type = 'flap & graft' then 'flap&graft'
when dclosure_type = 'flap / graft' then 'flap/graft'
when dclosure_type = 'flap/graft' then 'flap/graft'
when dclosure_type = 'flapo' then 'flap'
when dclosure_type = 'flaps' then 'flap'
when dclosure_type = 'flp' then 'flap'
when dclosure_type = 'graft' then 'graft'
when dclosure_type = 'graft & flap' then 'flap&graft'
when dclosure_type = 'graft / flap' then 'flap/graft'
when dclosure_type = 'graft/flap' then 'flap/graft'
when dclosure_type = 'grap' then 'graft'
when dclosure_type = 'grfat' then 'graft'
when dclosure_type = 'h graaft' then 'graft'
when dclosure_type = 'h graf' then 'graft'
when dclosure_type = 'h grafft' then 'graft'
when dclosure_type = 'h graft' then 'graft'
when dclosure_type = 'h.graft' then 'graft'
when dclosure_type = 'halo graft' then 'graft'
when dclosure_type = 'hgraft' then 'graft'
when dclosure_type = 'iopsy' then 'biospy'
when dclosure_type = 'keystone flap' then 'flap'
when dclosure_type = 'lfap' then 'flap'
when dclosure_type = 'sahve' then 'shave'
when dclosure_type = 'save' then 'shave'
when dclosure_type = 'scross' then 'shave'
when dclosure_type = 'shanve' then 'shave'
when dclosure_type = 'shave' then 'shave'
when dclosure_type = 'shave biopsy' then 'biospy'
when dclosure_type = 'shve' then 'shave'
when dclosure_type = '√ & graft' then 'flap&graft'
when dclosure_type = '√ graft' then 'flap&graft'
when dclosure_type = '√/graft' then 'flap/graft'
else 'excision' end as procedure_type,

case when wound_infection is not null then 'Wound/Infection'
        when prohylatic_antibiotics is not null then 'Prophylactic Antibiotic'
        when incomplete_excision is not null then 'Incomplete Excision' else 'No Complications' end as surgical_results

from (
select *, regexp_replace(lower(site),'[^a-z]+','') as derived_site, trim(lower(closure_type)) as dclosure_type from combined_data ) x 
where lower(date_of_treatment) not in ('incomplete','infection','date')
and file_year is not null
and lower(name) rlike '[a-z]{3}'

""")

# combined_data2.show(100)
combined_data2.createOrReplaceTempView("combined_data2")

# age_group was added to analyse the patterns
# subtype_cpy was added to find the 'pre-cancerous' situation on 09/03/2023
# added approx_year_of_birth to identify unique patients (this was after discussion on how to resolve duplicates with Santosh)

combined_data3 = spark.sql("""
select *, 
case when age <= 30 then '0-30'
         when age between 31 and 40 then '31-40'
         when age between 41 and 50 then '41-50'
         when age between 51 and 60 then '51-60'
         when age between 61 and 70 then '61-70'
         when age between 71 and 80 then '71-80'
         when age between 81 and 90 then '81-90'
         when age > 90 then '>90' else 'N/A' end as age_group,

case when other rlike 'actinic' and other rlike 'in situ|insitu' then 'Actinic Keratosis;In Situ-SCC'
    when other rlike 'actinic' and other rlike 'nodular' then 'Actinic Keratosis;Nodular-BCC'
    when other rlike 'actinic' and other rlike 'superficial' then 'Actinic Keratosis;Superficial-BCC'
    when other rlike 'nodular' and other rlike 'micronodular' then 'Nodular-BCC;Micronodular-BCC'
    when other rlike 'in situ|insitu' and other rlike 'nodular' then 'In Situ-SCC;Nodular-BCC'
    when other rlike 'superficial' and other rlike 'micronodular' then 'Superficial-BCC;Micronodular-BCC'
    else sub_type end as subtype_cpy ,
 cast(file_year as int) - age as approx_year_of_birth 
from combined_data2
""")

combined_data3.show(300)

output_csv_file = folder_csv + 'transformed_data.csv'
combined_data3.write.csv(output_csv_file, header=True, mode='overwrite')


# Santosh guided me not to include the postgres database details in the code
# He gave code to read from file and write it to the database

with open("/ssd/workarea/skin_cancer/code/postgres_properties.json", "r") as json_file:
	pg_properties = json.load(json_file)

	combined_data3.write.format("jdbc").option("url", pg_properties["url"]). \
	option("driver", "org.postgresql.Driver").option("dbtable", pg_properties["table"]) \
	.option("user", pg_properties["username"]) \
	.option("password", pg_properties["password"]) \
	.option("batchsize", "30000") \
	.option("truncate", "true") \
	.mode("overwrite") \
	.save()


spark.stop()



