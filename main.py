import pandas as pd
import json
import requests

# KEYS
api_key = 'AIzaSyDaI2OWjk0Y62jgKL2_UQWy5eTOEOEOpIs'

# IDs
video_ids = pd.read_csv('C:/XXX',
                       index_col=0,
                       sep=",",
                       encoding='latin1'
                       )
video_ids = video_ids['video_id'].to_list()

# SPECIFYING THE PATH FOR SAVING
path = 'C:\\Users\\Aaron Philipp\\Documents\\'
