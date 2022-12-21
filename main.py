import pandas as pd
import json
import requests

# KEYS
api_key = 'KEY'

# IDs
channel_ids = pd.read_csv('C:/Users/XXX.csv',
                       index_col=0,
                       sep=",",
                       encoding='latin1'
                       )

channel_ids = channel_ids['Channel ID'].to_list()

len(channel_ids)

# SPECIFYING THE PATH FOR SAVING
path = 'C:\\Users\\Aaron Philipp\\Documents'
