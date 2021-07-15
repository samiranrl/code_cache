
import pandas as pd
import numpy as np

pd.set_option('display.max_columns', 50)
pd.set_option('display.min_rows', 100)
pd.set_option('display.max_colwidth', -1)

from tqdm import tqdm
tqdm.pandas()

df = pd.read_csv('filename.csv', sep = '|', error_bad_lines = False)

cleanFloat = lambda x: float(x if x.isdigit() else 0)
cleanString = lambda x: str(x if x.isalpha() else '')

dict_convert = {1:cleanFloat, 2:cleanString,}


def normalize(x):
    x = str(x).lower().strip()
    x = re.sub(r'[1-9]','0', x)
    x =  re.sub(r' +',' ', x)
    return x

import re
df['text_field'] = df['unprocessed_text_field'].apply(normalize )

# freq counts of transaction
df['freq'] = df.groupby(['text_field', 'another_text_field'])['another_text_field'].transform('count')
df.drop_duplicates(['text_field', 'another_text_field'], inplace = True)

# redup

full = pd.DataFrame(full.values.repeat(full.freq, axis=0), columns=full.columns)


# interarrival
user_interarrival_times = list(user_df.date.sort_values().diff()[1:].apply(lambda x: x.days))
df['frac_freq'] = df.groupby(['text_field', 'another_text_field'])['another_text_field'].transform('count')

# handling dates

import pandas as pd


from datetime import date, datetime, timedelta
from dateutil.rrule import rrule, DAILY

df = pd.read_csv('data.csv', sep = '|')

delta_cd_window = 120
delta_rolling_window = 15
delta_last_good_point_window = 7



df.columns = ['date', 'merchant', 'transactions']


df['date'] = pd.to_datetime(df['date'])

start_date = datetime.today()- timedelta(days=delta_last_good_point_window + delta_cd_window + delta_rolling_window)
end_date = datetime.today()- timedelta(days=delta_last_good_point_window)


a = date(2009, 5, 30)
b = date(2009, 6, 9)

for dt in rrule(DAILY, dtstart=a, until=b):
    print dt.strftime("%Y-%m-%d")

    
    
# quantile/decile

# importing the modules
import pandas as pd
import numpy as np
    
# creating a DataFrame
df = {'Name' : ['Amit', 'Darren', 'Cody', 'Drew',
                'Ravi', 'Donald', 'Amy'],
      'Score' : [50, 71, 87, 95, 63, 32, 80]}
df = pd.DataFrame(df, columns = ['Name', 'Score'])
  
# adding Decile_rank column to the DataFrame
df['Decile_rank'] = pd.qcut(df['Score'], 10,
                            labels = False)
  
# printing the DataFrame
print(df)
