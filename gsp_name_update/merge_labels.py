
# import the files
import pandas as pd

#importing file with new ids
new_df = pd.read_csv('gsp_new_ids.csv')
new_df.head()

#importing file with old ids
old_df = pd.read_csv('gsp_old_ids.csv') 
old_df.head()


# joining them up on gsp_name to get readable name
joined_df = new_df.merge(old_df, on=['gsp_name'])
joined_df.head()