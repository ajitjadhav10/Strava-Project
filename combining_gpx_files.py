import glob
import pandas as pd
import os


os.chdir('/Users/ajit/Desktop/Airflow Project/export_100231341/OutputFiles/')

extension='csv'
all_filenames=[i for i in glob.glob('*.{}'.format(extension))]

#combine all files in the list
combined_csv=pd.concat([pd.read_csv(f) for f in all_filenames])

#export to csv
combined_csv.to_csv("All_Data.csv",index=False, encoding='utf-8-sig')