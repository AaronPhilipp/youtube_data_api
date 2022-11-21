import pandas as pd
import requests
import datetime
import time
from main import api_key
from main import path
from main import video_id

def get_all_video_comments(video_id,api_key):
    # build dataframe
    df = pd.DataFrame(columns=[
        'video_id',
        'comment_id',
        'channel_id',
        'channel_name',
        'content',
        'content_enc',
        'profile_image',
        'like_count',
        'published_date',
        'replies_count',
        'type',
        'parent_id',
        'timestamp'
    ])

    url = 'https://youtube.googleapis.com/youtube/v3/commentThreads?part=id&part=replies&part=snippet&maxResults=100&order=time&videoId=' + video_id + '&key=' + api_key

    response = requests.get(url=url).json()
    time.sleep(0.01) # short break after request

    if 'nextPageToken' not in response:
        page_token = []
    elif 'nextPageToken' in response:
        page_token = response['nextPageToken']

    if 'error' in response:

        comment_id = ''

        channel_name = ''

        channel_id = ''

        content = ''

        content_enc = ''

        profile_image = ''

        like_count = ''

        published_date = ''

        replies_count = ''

        type = 'errorcode' + str(response['error']['code']) + ':' + response['error']['errors'][0]['reason']

        parent_id = ''

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        df = pd.concat([df, pd.DataFrame([{
            'video_id': video_id,
            'comment_id': comment_id,
            'channel_name': channel_name,
            'channel_id': channel_id,
            'content': content,
            'content_enc': content_enc,
            'profile_image': profile_image,
            'like_count': like_count,
            'published_date': published_date,
            'replies_count': replies_count,
            'type': type,
            'parent_id': parent_id,
            'timestamp': timestamp
        }])], ignore_index=True)

    elif 'error' not in response:

        for i in response['items']:
            # add an try/except statement if a KeyError occurs: We need this so the script doesn't exit
            video_id = i['snippet']['videoId']

            comment_id = i['snippet']['topLevelComment']['id']

            channel_name = i['snippet']['topLevelComment']['snippet']['authorDisplayName']

            if 'authorChannelId' in i['snippet']['topLevelComment']['snippet']:
                channel_id = i['snippet']['topLevelComment']['snippet']['authorChannelId']['value']
            elif 'authorChannelId' not in i['snippet']['topLevelComment']['snippet']:
                channel_id = ''
            # channel_id = i['snippet']['topLevelComment']['snippet']['authorChannelId']['value']

            content = i['snippet']['topLevelComment']['snippet']['textOriginal']
            content_enc = content.encode('raw-unicode-escape')

            profile_image = i['snippet']['topLevelComment']['snippet']['authorProfileImageUrl']

            like_count = i['snippet']['topLevelComment']['snippet']['likeCount']

            published_date = i['snippet']['topLevelComment']['snippet']['publishedAt']

            replies_count = i['snippet']['totalReplyCount']

            type = 'comment'

            parent_id = ''

            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            df = pd.concat([df, pd.DataFrame([{
                'video_id': video_id,
                'comment_id': comment_id,
                'channel_name': channel_name,
                'channel_id': channel_id,
                'content': content,
                'content_enc': content_enc,
                'profile_image': profile_image,
                'like_count': like_count,
                'published_date': published_date,
                'replies_count': replies_count,
                'type': type,
                'parent_id': parent_id,
                'timestamp': timestamp
            }])], ignore_index=True)

            if replies_count > 0:
                url2 = 'https://youtube.googleapis.com/youtube/v3/comments?part=snippet&part=id&maxResults=100&parentId=' + comment_id + '&key=' + api_key

                response2 = requests.get(url=url2).json()
                time.sleep(0.01)  # short break after request

                for i in response2['items']:
                    comment_id = i['id']

                    channel_name = i['snippet']['authorDisplayName']

                    channel_id = i['snippet']['authorChannelId']['value']

                    content = i['snippet']['textDisplay']
                    content_enc = content.encode('raw-unicode-escape')

                    profile_image = i['snippet']['authorProfileImageUrl']

                    like_count = i['snippet']['likeCount']

                    published_date = i['snippet']['publishedAt']

                    parent_id = i['snippet']['parentId']

                    replies_count = ''

                    type = 'reply'

                    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    df = pd.concat([df, pd.DataFrame([{
                        'video_id': video_id,
                        'comment_id': comment_id,
                        'channel_name': channel_name,
                        'channel_id': channel_id,
                        'content': content,
                        'content_enc': content_enc,
                        'profile_image': profile_image,
                        'like_count': like_count,
                        'published_date': published_date,
                        'replies_count': replies_count,
                        'type': type,
                        'parent_id': parent_id,
                        'timestamp': timestamp
                    }])], ignore_index=True)

        while page_token != []:

            url = 'https://youtube.googleapis.com/youtube/v3/commentThreads?part=id&part=replies&part=snippet&maxResults=100&order=time&videoId=' + video_id + '&key=' + api_key + '&pageToken=' + page_token

            response = requests.get(url=url).json()
            time.sleep(0.01)  # short break after request

            # If there are more than 50 results, the response gives back a nextPageToken to get the other results, so we need to save this Token
            if 'nextPageToken' not in response:
                page_token = []
            elif 'nextPageToken' in response:
                page_token = response['nextPageToken']

            for i in response['items']:
                # add an try/except statement if a KeyError occurs: We need this so the script doesn't exit
                video_id = i['snippet']['videoId']

                comment_id = i['snippet']['topLevelComment']['id']

                channel_name = i['snippet']['topLevelComment']['snippet']['authorDisplayName']
                if 'authorChannelId' in i['snippet']['topLevelComment']['snippet']:
                    channel_id = i['snippet']['topLevelComment']['snippet']['authorChannelId']['value']
                elif 'authorChannelId' not in i['snippet']['topLevelComment']['snippet']:
                    channel_id = ''
                # channel_id = i['snippet']['topLevelComment']['snippet']['authorChannelId']['value']

                content = i['snippet']['topLevelComment']['snippet']['textOriginal']
                content_enc = content.encode('raw-unicode-escape')

                profile_image = i['snippet']['topLevelComment']['snippet']['authorProfileImageUrl']

                like_count = i['snippet']['topLevelComment']['snippet']['likeCount']

                published_date = i['snippet']['topLevelComment']['snippet']['publishedAt']

                replies_count = i['snippet']['totalReplyCount']

                type = 'comment'

                parent_id = ''

                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                df = pd.concat([df, pd.DataFrame([{
                    'video_id': video_id,
                    'comment_id': comment_id,
                    'channel_name': channel_name,
                    'channel_id': channel_id,
                    'content': content,
                    'content_enc': content_enc,
                    'profile_image': profile_image,
                    'like_count': like_count,
                    'published_date': published_date,
                    'replies_count': replies_count,
                    'type': type,
                    'parent_id': parent_id,
                    'timestamp': timestamp
                }])], ignore_index=True)

                if replies_count > 0:
                    url2 = 'https://youtube.googleapis.com/youtube/v3/comments?part=snippet&part=id&maxResults=100&parentId=' + comment_id + '&key=' + api_key

                    response2 = requests.get(url=url2).json()
                    time.sleep(0.01)  # short break after request

                    for i in response2['items']:
                        comment_id = i['id']

                        channel_name = i['snippet']['authorDisplayName']

                        channel_id = i['snippet']['authorChannelId']['value']

                        content = i['snippet']['textDisplay']
                        content_enc = content.encode('raw-unicode-escape')

                        profile_image = i['snippet']['authorProfileImageUrl']

                        like_count = i['snippet']['likeCount']

                        published_date = i['snippet']['publishedAt']

                        parent_id = i['snippet']['parentId']

                        replies_count = ''

                        type = 'reply'

                        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        df = pd.concat([df, pd.DataFrame([{
                            'video_id': video_id,
                            'comment_id': comment_id,
                            'channel_name': channel_name,
                            'channel_id': channel_id,
                            'content': content,
                            'content_enc': content_enc,
                            'profile_image': profile_image,
                            'like_count': like_count,
                            'published_date': published_date,
                            'replies_count': replies_count,
                            'type': type,
                            'parent_id': parent_id,
                            'timestamp': timestamp
                        }])], ignore_index=True)

    return df


df = get_all_video_comments(video_id=video_id, api_key=api_key)

df.to_csv((path + 'XXX.csv'), encoding='utf-8-sig', escapechar='\\')


# GET COMMENTS OF MULTIPLE VIDEOS
# big = pd.DataFrame()
# video_ids = ['video_id1','video_id2',...]
#
# for i in video_ids_list:
#     df = get_all_video_comments(video_id=i, api_key=api_key)
#     big = pd.concat([big,df], ignore_index=True)
#     big.to_csv((path + 'XXX.csv'),  encoding='utf-8-sig', escapechar='\\')