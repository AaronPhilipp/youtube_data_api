import pandas as pd
import requests
import datetime
import time
import os
from main import api_key
from main import path
from main import channel_ids


# CHANNEL INFORMATIONS
# You can only pass single channel_ids. If you have a list you have to use the function in a for-loop.


def get_channel_informations(channel_id, api_key):
    df = pd.DataFrame(columns=['channel_id',
                               'channel_title',
                               'channel_description',
                               'publication_date',
                               'view_count',
                               'subscriber_count',
                               'video_count',
                               'topic_categories',
                               'keywords',
                               'thumbnail_url',
                               'timestamp'
                               ]
                      )

    url = 'https://youtube.googleapis.com/youtube/v3/channels?part=id&part=snippet&part=statistics&part=topicDetails&part=contentDetails&part=brandingSettings&id=' + channel_id + '&maxResults=20&pageToken=' + 'None' + '&key=' + api_key

    response = requests.get(url=url).json()
    time.sleep(0.01)  # short break after request

    if 'error' in response:
        raise TypeError(response['error']['message'])

    # Some channels don't exist anymore. We need this if-else-statement to not exit the script.
    if 'items' in response:
        channel_id = response['items'][0]['id']
        channel_title = response['items'][0]['snippet']['title']
        channel_description = response['items'][0]['snippet']['description']
        publication_date = response['items'][0]['snippet']['publishedAt']
        view_count = response['items'][0]['statistics']['viewCount']
        subs_count = response['items'][0]['statistics']['subscriberCount']
        video_count = response['items'][0]['statistics']['videoCount']
        if 'topicDetails' not in response['items'][0]:
            topic_categories = ''
        elif 'topicDetails' in response['items'][0]:
            if 'topicCategories' not in response['items'][0]['topicDetails']:
                topic_categories = ''
            elif 'topicCategories' in response['items'][0]['topicDetails']:
                topic_categories = response['items'][0]['topicDetails']['topicCategories']
        if 'keywords' not in response['items'][0]['brandingSettings']['channel']:
            keywords = ''
        elif 'keywords' in response['items'][0]['brandingSettings']['channel']:
            keywords = response['items'][0]['brandingSettings']['channel']['keywords']
        thumbnail_url = response['items'][0]['snippet']['thumbnails']['high']['url']
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    elif 'items' not in response:
        channel_id = channel_id
        channel_title = ''
        channel_description = ''
        publication_date = ''
        view_count = ''
        subs_count = ''
        video_count = ''
        topic_categories = ''
        keywords = ''
        thumbnail_url = ''
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df = pd.concat([df, pd.DataFrame([{
        'channel_id': channel_id,
        'channel_title': channel_title,
        'channel_description': channel_description,
        'publication_date': publication_date,
        'view_count': view_count,
        'subscriber_count': subs_count,
        'video_count': video_count,
        'topic_categories': topic_categories,
        'keywords': keywords,
        'thumbnail_url': thumbnail_url,
        'timestamp': timestamp
    }])], ignore_index=True)

    return df


# GET CHANNEL INFORMATION FROM SINGLE CHANNEL

# df = get_channel_informations(channel_id=channel_id, api_key=api_key)
# df.to_csv((path + 'XXX.csv'), encoding='utf-8-sig')


# GET CHANNEL INFORMATIONS FOR MULTIPLE CHANNELS
channel_ids_list = channel_ids['channel_id'].to_list()

for i in channel_ids_list:
    tmp = get_channel_informations(channel_id=i, api_key=api_key)
    if os.path.isfile(path + 'channel_informations_' + time.strftime("%Y-%m-%d") + '.csv'):
        df = pd.read_csv(path + 'channel_informations_' + time.strftime("%Y-%m-%d") + '.csv',
                         index_col=0)
        df = pd.concat([df, tmp], ignore_index=True)
        df.to_csv((path + 'channel_informations_' + time.strftime("%Y-%m-%d") + '.csv'),
                  encoding='utf-8-sig')
    else:
        tmp.to_csv((path + 'channel_informations_' + time.strftime("%Y-%m-%d") + '.csv'),
                   encoding='utf-8-sig')
    channel_ids = channel_ids[channel_ids['channel_id'] != i]
    channel_ids.to_csv('C:\\Users\\XXX\\'
                       'channel_ids_for_channel_informations__left.csv')
