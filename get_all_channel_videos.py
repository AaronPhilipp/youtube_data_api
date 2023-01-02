import pandas as pd
import requests
import datetime
import time
from main import api_key
from main import path
from main import channel_ids


def get_all_channel_videos(channel_id, api_key):
    df = pd.DataFrame(columns=['channel_id',
                               'video_id',
                               'video_title',
                               'video_description',
                               'video_thumbnail',
                               'view_count',
                               'like_count',
                               'favorite_count',
                               'comments_count',
                               'duration',
                               'tags',
                               'publication_date',
                               'livebroadcastcontent',
                               'timestamp'
                               ]
                      )

    # all uploads of a channel are stored in the so called upload_playlist.
    # To get the link to this playlist you only need to replace the 'C' in the 2nd place with 'U'.
    playlist_id = channel_id[:1] + 'U' + channel_id[2:]

    url = 'https://youtube.googleapis.com/youtube/v3/playlistItems?part=id&part=snippet&part=status&playlistId=' + playlist_id + '&maxResults=50&key=' + api_key
    response = requests.get(url=url).json()
    time.sleep(0.01)  # short break after request

    # If there are more than 50 results, the response gives back a nextPageToken to get the other results, so we need to save this Token
    if 'nextPageToken' not in response:
        page_token = []
    elif 'nextPageToken' in response:
        page_token = response['nextPageToken']

    if 'error' in response:
        raise TypeError(response['error']['message'])

    if 'items' not in response:
        raise TypeError('A problem with the channel_id' + channel_id + 'occured.')

    elif 'items' in response:

        # going through each item on the first page
        for i in response['items']:

            # add an try/except statement if a KeyError occurs: We need this so the script doesn't exit
            try:
                channel_id = i['snippet']['channelId']
            except KeyError:
                channel_id = ''

            try:
                video_id = i['snippet']['resourceId']['videoId']
            except KeyError:
                video_id = ''

            try:
                publication_date = i['snippet']['publishedAt']
            except KeyError:
                publication_date = ''

            try:
                video_title = i['snippet']['title']
            except KeyError:
                video_title = ''

            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


            url2 = 'https://youtube.googleapis.com/youtube/v3/videos?part=snippet&part=id&part=statistics&part=localizations&part=status&part=contentDetails&part=liveStreamingDetails&part=topicDetails&id=' + video_id + '&maxResults=50&key=' + api_key
            response2 = requests.get(url=url2).json()
            time.sleep(0.01)  # short break after request

            if 'error' in response:
                raise TypeError(response['error']['message'])

            try:
                if 'description' not in response2['items'][0]['snippet']:
                    video_description = ''
                elif 'description' in response2['items'][0]['snippet']:
                    video_description = response2['items'][0]['snippet']['description']
            except KeyError:
                video_description = ''

            # sometimes there is no thumbnail in maxres available. If this is the case, we need to look for other resolutions
            try:
                if 'maxres' in response2['items'][0]['snippet']['thumbnails']:
                    video_thumbnail = response2['items'][0]['snippet']['thumbnails']['maxres']['url']
                elif 'high' in response2['items'][0]['snippet']['thumbnails']:
                    video_thumbnail = response2['items'][0]['snippet']['thumbnails']['high']['url']
                elif 'medium' in response2['items'][0]['snippet']['thumbnails']:
                    video_thumbnail = response2['items'][0]['snippet']['thumbnails']['medium']['url']
                elif 'default' in response2['items'][0]['snippet']['thumbnails']:
                    video_thumbnail = response2['items'][0]['snippet']['thumbnails']['default']['url']
                elif 'default' not in response2['items'][0]['snippet']['thumbnails']:
                    video_thumbnail = ''
            except KeyError:
                video_thumbnail = ''

            try:
                if 'viewCount' not in response2['items'][0]['statistics']:
                    view_count = ''
                elif 'viewCount' in response2['items'][0]['statistics']:
                    view_count = response2['items'][0]['statistics']['viewCount']
            except KeyError:
                view_count = ''

            try:
                if 'likeCount' not in response2['items'][0]['statistics']:
                    like_count = ''
                elif 'likeCount' in response2['items'][0]['statistics']:
                    like_count = response2['items'][0]['statistics']['likeCount']
            except KeyError:
                like_count = ''

            try:
                if 'favoriteCount' not in response2['items'][0]['statistics']:
                    fav_count = ''
                elif 'favoriteCount' in response2['items'][0]['statistics']:
                    fav_count = response2['items'][0]['statistics']['favoriteCount']
            except KeyError:
                fav_count = ''

            try:
                if 'commentCount' not in response2['items'][0]['statistics']:
                    comments_count = ''
                elif 'commentCount' in response2['items'][0]['statistics']:
                    comments_count = response2['items'][0]['statistics']['commentCount']
            except KeyError:
                comments_count = ''

            try:
                if 'duration' not in response2['items'][0]['contentDetails']:
                    vid_duration = ''
                elif 'duration' in response2['items'][0]['contentDetails']:
                    vid_duration = response2['items'][0]['contentDetails']['duration']
            except KeyError:
                vid_duration = ''

            try:
                if 'tags' not in response2['items'][0]['snippet']:  # TODO: Maybe there's a better formation than the original brackets.
                    video_tags = ''
                elif 'tags' in response2['items'][0]['snippet']:
                    video_tags = response2['items'][0]['snippet']['tags']
            except KeyError:
                video_tags = ''

            try:
                if 'liveBroadcastContent' not in response2['items'][0]['snippet']:
                    livebroadcastcontent = ''
                elif 'liveBroadcastContent' in response2['items'][0]['snippet']:
                    livebroadcastcontent = response2['items'][0]['snippet']['liveBroadcastContent']
            except KeyError:
                livebroadcastcontent = ''

            df = pd.concat([df, pd.DataFrame([{
                'channel_id': '"' + channel_id + '"',
                'video_id': '"' + video_id + '"',
                'video_title': '"' + video_title + '"',
                'publication_date': publication_date,
                'video_description': '"' + video_description + '"',
                'video_thumbnail': video_thumbnail,
                'view_count': view_count,
                'like_count': like_count,
                'favorite_count': fav_count,
                'comments_count': comments_count,
                'duration': vid_duration,
                'tags': video_tags,
                'livebroadcastcontent': '"' + livebroadcastcontent + '"',
                'timestamp': timestamp
            }])], ignore_index=True)


    while page_token != []:
        url = 'https://youtube.googleapis.com/youtube/v3/playlistItems?part=id&part=snippet&part=status&playlistId=' + playlist_id + '&maxResults=50&key=' + api_key + '&pageToken=' + page_token
        response = requests.get(url=url).json()
        time.sleep(0.01)  # short break after request

        if 'nextPageToken' not in response:
            page_token = []
        elif 'nextPageToken' in response:
            page_token = response['nextPageToken']

        if 'error' in response:
            raise TypeError(response['error']['message'])

        if 'items' not in response:
            raise TypeError('A problem with the channel_id' + channel_id)

        elif 'items' in response:

            # going through each item on the first page
            for i in response['items']:

                # add an try/except statement if a KeyError occurs: We need this so the script doesn't exit
                try:
                    channel_id = i['snippet']['channelId']
                except KeyError:
                    channel_id = ''

                try:
                    video_id = i['snippet']['resourceId']['videoId']
                except KeyError:
                    video_id = ''

                try:
                    publication_date = i['snippet']['publishedAt']
                except KeyError:
                    publication_date = ''

                try:
                    video_title = i['snippet']['title']
                except KeyError:
                    video_title = ''

                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                url2 = 'https://youtube.googleapis.com/youtube/v3/videos?part=snippet&part=id&part=statistics&part=localizations&part=status&part=contentDetails&part=liveStreamingDetails&part=topicDetails&id=' + video_id + '&maxResults=50&key=' + api_key
                response2 = requests.get(url=url2).json()
                time.sleep(0.01)  # short break after request

                if 'error' in response:
                    raise TypeError(response['error']['message'])

                try:
                    if 'description' not in response2['items'][0]['snippet']:
                        video_description = ''
                    elif 'description' in response2['items'][0]['snippet']:
                        video_description = response2['items'][0]['snippet']['description']
                except KeyError:
                    video_description = ''

                # sometimes there is no thumbnail in maxres available. If this is the case, we need to look for other resolutions
                try:
                    if 'maxres' in response2['items'][0]['snippet']['thumbnails']:
                        video_thumbnail = response2['items'][0]['snippet']['thumbnails']['maxres']['url']
                    elif 'high' in response2['items'][0]['snippet']['thumbnails']:
                        video_thumbnail = response2['items'][0]['snippet']['thumbnails']['high']['url']
                    elif 'medium' in response2['items'][0]['snippet']['thumbnails']:
                        video_thumbnail = response2['items'][0]['snippet']['thumbnails']['medium']['url']
                    elif 'default' in response2['items'][0]['snippet']['thumbnails']:
                        video_thumbnail = response2['items'][0]['snippet']['thumbnails']['default']['url']
                    elif 'default' not in response2['items'][0]['snippet']['thumbnails']:
                        video_thumbnail = ''
                except KeyError:
                    video_thumbnail = ''

                try:
                    if 'viewCount' not in response2['items'][0]['statistics']:
                        view_count = ''
                    elif 'viewCount' in response2['items'][0]['statistics']:
                        view_count = response2['items'][0]['statistics']['viewCount']
                except KeyError:
                    view_count = ''

                try:
                    if 'likeCount' not in response2['items'][0]['statistics']:
                        like_count = ''
                    elif 'likeCount' in response2['items'][0]['statistics']:
                        like_count = response2['items'][0]['statistics']['likeCount']
                except KeyError:
                    like_count = ''

                try:
                    if 'favoriteCount' not in response2['items'][0]['statistics']:
                        fav_count = ''
                    elif 'favoriteCount' in response2['items'][0]['statistics']:
                        fav_count = response2['items'][0]['statistics']['favoriteCount']
                except KeyError:
                    fav_count = ''

                try:
                    if 'commentCount' not in response2['items'][0]['statistics']:
                        comments_count = ''
                    elif 'commentCount' in response2['items'][0]['statistics']:
                        comments_count = response2['items'][0]['statistics']['commentCount']
                except KeyError:
                    comments_count = ''

                try:
                    if 'duration' not in response2['items'][0]['contentDetails']:
                        vid_duration = ''
                    elif 'duration' in response2['items'][0]['contentDetails']:
                        vid_duration = response2['items'][0]['contentDetails']['duration']
                except KeyError:
                    vid_duration = ''

                try:
                    if 'tags' not in response2['items'][0]['snippet']:
                        video_tags = ''
                    elif 'tags' in response2['items'][0]['snippet']:
                        video_tags = response2['items'][0]['snippet']['tags']  # TODO: Maybe there's a better formation than the original brackets.
                except KeyError:
                    video_tags = ''

                try:
                    if 'liveBroadcastContent' not in response2['items'][0]['snippet']:
                        livebroadcastcontent = ''
                    elif 'liveBroadcastContent' in response2['items'][0]['snippet']:
                        livebroadcastcontent = response2['items'][0]['snippet']['liveBroadcastContent']
                except KeyError:
                    livebroadcastcontent = ''

                df = pd.concat([df, pd.DataFrame([{
                'channel_id': '"' + channel_id + '"',
                    'video_id': '"' + video_id + '"',
                    'video_title': '"' + video_title + '"',
                    'publication_date': publication_date,
                    'video_description': '"' + video_description + '"',
                    'video_thumbnail': video_thumbnail,
                    'view_count': view_count,
                    'like_count': like_count,
                    'favorite_count': fav_count,
                    'comments_count': comments_count,
                    'duration': vid_duration,
                    'tags': video_tags,
                    'livebroadcastcontent': '"' + livebroadcastcontent + '"',
                    'timestamp': timestamp
                }])], ignore_index=True)

    return df

# channel_id = 'XXX'
# df = get_all_channel_videos(channel_id=channel_id, api_key=api_key)
# df.to_csv((path + 'XXX.csv'), encoding='utf-8-sig')

channel_ids_list = channel_ids['channel_id'].to_list()

# GET VIDEO INFORMATIONS OF MULTIPLE CHANNELS

big = pd.DataFrame()

for i in channel_ids_list:
    df = get_all_channel_videos(channel_id=i, api_key=api_key)
    big = pd.concat([big, df], ignore_index=True)
    big.to_csv((path + 'video_informations_2023-01-02_1.csv'),  encoding='utf-8-sig')
    channel_ids = channel_ids[channel_ids['channel_id'] != i]
    channel_ids.to_csv('C:\\Users\\XXX.csv')
