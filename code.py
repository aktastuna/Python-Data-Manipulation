Data Source (https://data.ibb.gov.tr/dataset/otomatik-meteorolojik-gozlem-istasyon-verileri/resource/9e4fd600-0e93-45c6-8807-0ac1c2ae90a1)

import numpy as np
import pandas as pd
import os
import zipfile
from datetime import datetime, timedelta

# =============================================================================
# Part 1: Read all the sheets of an Excel file and keep them in a dataframe and convert it to csv file
# =============================================================================

path = r'C:\Users\tunahan.aktas\Desktop\ibb_veri/'
master_file = 'otomatik-meteorolojik-gozlem-istasyon-verileri'
file_type_excel = '.xlsx'

# Sheet_name is the key point because we don't parse any specific sheets.
list_of_sheets = pd.read_excel(path + master_file + file_type_excel, sheet_name = None)

#In order to list the sheets, I used .keys()
list_of_sheets = list(list_of_sheets.keys())

#I don't want to append the first sheet because it doesn't contain any important information.
#I want to add the name of the sheet as variable because I might need it. I inserted them into the first column of my df.

df = pd.DataFrame()
for i in list_of_sheets:
    if i != 'Metadata':
        temporary_df = pd.read_excel(path + master_file + file_type_excel, sheet_name = i)
        temporary_df.insert(loc = 0, column = 'neighbourhood', value = i)
        df = df.append(temporary_df, ignore_index = True)
    else:
        continue
#In each sheets, first two columns were provided as merged from the data provider. That's why the second row of each data
#doesn't contain an useful information. I just dropped them.

df.dropna(subset = ['Verinin Yılı'], axis = 0, inplace = True)
df.to_csv(os.path.join(os.path.dirname(path), master_file + '.csv'), header = True, index = False)

# =============================================================================
# Part 2: Read the data from Zip File and manipulate
# =============================================================================

path = r'C:\Users\tunahan.aktas\Desktop\ibb_veri/'
master_file = 'otomatik-meteorolojik-gozlem-istasyon-verileri'
file_type = '.csv'
zip_file_name = master_file
zip_file_type = '.zip'

#I need to remove Turkish characters from my dataframe.
turkish_characters = {'ı': 'i',
                      'ö': 'o',
                      'ü': 'u'}
#I also need to replace Turkish momths with English months.
months = {'Ocak': 'January', 
          'Şubat': 'February', 
          'Mart': 'March', 
          'Nisan': 'April', 
          'Mayıs': 'May', 
          'Haziran': 'June', 
          'Temmuz': 'July',
          'Ağustos': 'August', 
          'Eylül': 'September', 
          'Ekim': 'October', 
          'Kasım': 'November', 
          'Aralık': 'December'}

#Let's read directly from ZipFile.
zf = zipfile.ZipFile(path + zip_file_name + zip_file_type)
df = pd.read_csv(zf.open(master_file + file_type))
new_cols = pd.Series(df.columns)

#I don't want weird characters in my columns.
new_cols = new_cols.str.replace('/', '').str.strip().str.replace('  ', '_').str.replace(' ', '_').str.lower()
for turkish, english in turkish_characters.items():
    new_cols = new_cols.str.replace(turkish, english)
df.columns = new_cols

for tr_date, eng_date in months.items():
    df['gun_ay'] = df['gun_ay'].str.replace(tr_date, eng_date)
df.insert(loc = 1, column = 'revised_date', 
          value = df['gun_ay'] + ' ' + df['verinin_yili'].astype(str).str.split('.').str[0])

df.drop(df[df['revised_date'] == ' Gün / Ay Verinin Yılı'].index, axis = 0, inplace = True)
df['revised_date'] = pd.to_datetime(df['revised_date'], format = '%d %B %Y', dayfirst = True, errors = 'coerce')
df.drop(['gun_ay', 'verinin_yili'], axis = 1, inplace = True)
df.reset_index(drop = True, inplace = True)
df.replace({'///': np.nan}, value = None, inplace = True)
df.rename(columns = {'maks_nemz': 'maks_nem_z'}, inplace = True)

def removal(x):
    extraction = pd.DataFrame()
    for i in x.columns:
        if i[-2:] != '_z' and i not in (['neighbourhood', 'revised_date']):
            temporary_df = x[x[i].str.contains(':', na = False)]
            extraction = extraction.append(temporary_df)
        else:
            pass
    x.drop(index = extraction.index, axis = 0, inplace = True)
    x.reset_index(drop = True, inplace = True)
    return x
new_df = removal(df)

for i in new_df.columns:
    if i[-2:] == '_z':
        new_df[i] = pd.to_timedelta(new_df[i], errors = 'coerce')
    elif i in (['neighbourhood', 'revised_date']):
        pass
    else:
        new_df[i] = new_df[i].astype(float)
#Let's check if our variables have appropriate data type.
new_df.info()

# =============================================================================
# Part 3: Folders
# =============================================================================

#Now, let's create new folders from our 'neighbourhood' variable and divide the data accordingly and save them into those folders.

folder_list = new_df.drop_duplicates(subset = 'neighbourhood')['neighbourhood'].tolist()
for folder in folder_list:
    if not os.path.exists(path + folder):
        os.mkdir(path + folder)

multiple_files = new_df.groupby('neighbourhood')

for i, split_df in multiple_files:
    output_directory = os.path.join(r'C:\Users\tunahan.aktas\Desktop\ibb_veri', str(i) + '/')
    split_df.to_excel(output_directory + str(i) + file_type_excel, 
                      index = False, sheet_name = str(i))
