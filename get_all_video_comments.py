import os
import pandas as pd
import requests
import datetime
import time
import csv

# KEY
api_key = "XXX"

# IDs (read a csv with the video_ids, you want to scrape data)
# WORKAROUND: MAKE A NEW FILE THAT CONTAINS THE VIDEO_IDS AND KEEP THE ORIGINAL FILE UNTOUCHED (then you can overwrite this one when you save the remaining video_ids [last step in the for-loop])
video_ids = pd.read_csv(
    filepath_or_buffer="XXX.csv",
    index_col=None,  # we have no index column in the data
    header=0,  # the first row contains the header of the columns
    sep=",",
    encoding="latin1",
    dtype={
        "video_id": str,
    },  # specify the type of the columns
)

# TRANSFORM THE VIDEO_IDS IN A LIST
video_ids_list = video_ids["video_id"].to_list()

# SPECIFYING THE PATH FOR SAVING
path = "XXX\\"

# DEFINE THE FUNCTION
def get_all_video_information(video_id, api_key):
    df_def = pd.DataFrame(
        columns=[
            "channel_id",
            "video_id",
            "video_title",
            "video_description",
            "video_thumbnail",
            "view_count",
            "like_count",
            "favorite_count",
            "comments_count",
            "duration",
            "tags",
            "publication_date",
            "livebroadcastcontent",
            "timestamp",
        ]
    )

    url = (
        "https://youtube.googleapis.com/youtube/v3/videos?part=snippet&part=id&part=statistics&part="
        "localizations&part=status&part=contentDetails&part=liveStreamingDetails&part=topicDetails&id="
        + video_id
        + "&maxResults=50&key="
        + api_key
    )
    response = requests.get(url=url).json()
    time.sleep(0.01)  # short break after request

    if "error" in response:
        # if response2['error']['message'] == 'The playlist identified with the request\'s ' \
        #                                    '<code>playlistId</code> parameter cannot be found.':
        #     raise TypeError(response2['error']['message'])
        # else:
        raise ValueError(response["error"]["message"])

    elif "items" in response:
        channel_id = response["items"][0]["snippet"]["channelId"]
        video_title = response["items"][0]["snippet"]["title"]
        publication_date = response["items"][0]["snippet"]["publishedAt"]
        video_description = response["items"][0]["snippet"]["description"]

        # sometimes there is no thumbnail in maxres available. If this is the case,
        # we need to look for other resolutions
        if "maxres" in response["items"][0]["snippet"]["thumbnails"]:
            video_thumbnail = response["items"][0]["snippet"]["thumbnails"]["maxres"][
                "url"
            ]
        elif "high" in response["items"][0]["snippet"]["thumbnails"]:
            video_thumbnail = response["items"][0]["snippet"]["thumbnails"]["high"][
                "url"
            ]
        elif "medium" in response["items"][0]["snippet"]["thumbnails"]:
            video_thumbnail = response["items"][0]["snippet"]["thumbnails"]["medium"][
                "url"
            ]
        else:
            video_thumbnail = response["items"][0]["snippet"]["thumbnails"]["default"][
                "url"
            ]

        try:
            view_count = response["items"][0]["statistics"]["viewCount"]
        except KeyError:
            view_count = ""

        try:
            like_count = response["items"][0]["statistics"]["likeCount"]
        except KeyError:
            like_count = ""

        try:
            fav_count = response["items"][0]["statistics"]["favoriteCount"]
        except KeyError:
            fav_count = ""

        try:
            comments_count = response["items"][0]["statistics"]["commentCount"]
        except KeyError:
            comments_count = ""

        try:
            vid_duration = response["items"][0]["contentDetails"]["duration"]
        except KeyError:
            vid_duration = ""

        try:
            video_tags = response["items"][0]["snippet"]["tags"]
        except KeyError:
            video_tags = ""

        livebroadcastcontent = response["items"][0]["snippet"]["liveBroadcastContent"]
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        df_def = pd.concat(
            [
                df_def,
                pd.DataFrame(
                    [
                        {
                            "channel_id": channel_id,
                            "video_id": video_id,
                            "video_title": video_title,
                            "publication_date": publication_date,
                            "video_description": video_description,
                            "video_thumbnail": video_thumbnail,
                            "view_count": view_count,
                            "like_count": like_count,
                            "favorite_count": fav_count,
                            "comments_count": comments_count,
                            "duration": vid_duration,
                            "tags": video_tags,
                            "livebroadcastcontent": livebroadcastcontent,
                            "timestamp": timestamp,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )

    return df_def

# INITIAL COUNTER FOR THE SAVING PROCESS
counter = 0

# LOOP THROUGH THE LIST/VIDEO_IDS
for i in video_ids_list:
    try:
        tmp = get_all_video_information(video_id=i, api_key=api_key)
        print(i + ": ok (" + datetime.datetime.now().strftime("%H:%M:%S") + ")")
        if os.path.isfile(
            path
            + "video_information_"
            + time.strftime("%Y-%m-%d")
            + ".csv"
        ):
            df = pd.read_csv(
                path
                + "video_information_"
                + time.strftime("%Y-%m-%d")
                + ".csv",
                delimiter=",",
                encoding="utf-8-sig",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                doublequote=True,
                index_col=None,
                dtype={
                    "channel_id": str,
                    "video_id": str,
                    "video_title": str,
                    "video_description": str,
                    "publication_date": str,
                    "video_thumbnail": str,
                    "view_count": float,
                    "like_count": float,
                    "favorite_count": float,
                    "comments_count": float,
                    "duration": str,
                    "tags": str,
                    "livebroadcastcontent": str,
                    "timestamp": str,
                },
            )

            df = pd.concat([df, tmp], ignore_index=True)
            df.to_csv(
                path_or_buf=path
                + "video_information_"
                + time.strftime("%Y-%m-%d")
                + ".csv",
                sep=",",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                encoding="utf-8-sig",
                index=False,
            )
        else:
            tmp.to_csv(
                path_or_buf=path
                + "video_information_"
                + time.strftime("%Y-%m-%d")
                + ".csv",
                sep=",",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                encoding="utf-8-sig",
                index=False,
            )
    except TypeError:
        print(
            i
            + ": typeerror"
            + " ("
            + datetime.datetime.now().strftime("%H:%M:%S")
            + ")"
        )
        if os.path.isfile(path + "channel_ids_errors.csv"):
            df = pd.read_csv(
                path + "channel_ids_errors_.csv",
                delimiter=",",
                encoding="utf-8-sig",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                doublequote=True,
                index_col=None,
                dtype={"channel_id": str},
            )

            tmp = pd.DataFrame([{"channel_id": i}])

            df = pd.concat([df, tmp], ignore_index=True)
            df.to_csv(
                path + "channel_ids_errors.csv",
                sep=",",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                encoding="utf-8-sig",
                index=False,
            )
        else:
            tmp = pd.DataFrame([{"channel_id": i}])
            tmp.to_csv(
                path + "channel_ids_errors.csv",
                sep=",",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                encoding="utf-8-sig",
                index=False,
            )
    except IndexError:
        print(
            i
            + ": indexerror"
            + " ("
            + datetime.datetime.now().strftime("%H:%M:%S")
            + ")"
        )
        if os.path.isfile(path + "channel_ids_indexerrors.csv"):
            df = pd.read_csv(
                path + "channel_ids_indexerrors.csv",
                delimiter=",",
                encoding="utf-8-sig",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                doublequote=True,
                index_col=None,
                dtype={"channel_id": str},
            )

            tmp = pd.DataFrame([{"channel_id": i}])

            df = pd.concat([df, tmp], ignore_index=True)
            df.to_csv(
                path + "channel_ids_indexerrors.csv",
                sep=",",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                encoding="utf-8-sig",
                index=False,
            )
        else:
            tmp = pd.DataFrame([{"channel_id": i}])
            tmp.to_csv(
                path + "channel_ids_indexerrors.csv",
                sep=",",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                encoding="utf-8-sig",
                index=False,
            )

    # HERE WE SAVE THE REMAINING VIDEO_IDS (IF YOU NAME IT LIKE YOUR ORIGINAL VIDEO_ID-CSV IT WILL OVERWRITE THIS FILE)
    video_ids = video_ids[video_ids["video_id"] != i]
    print(str(len(video_ids)) + " videos remaining")
    video_ids.to_csv(
        path_or_buf="XXX.csv",
        sep=",",
        quotechar='"',
        quoting=csv.QUOTE_ALL,
        encoding="latin1",
        index=False,
    )
    
    # EVERY 100 VIDEOS THE SCRAPING PROCESS STOPS FOR THE 5 SECONDS TO STOP YOUR SCRIPT SO THAT YOU DON'T LOSE DATA (IF YOU STOP YOUR SCRIPT IN THE MIDDLE OF THE SAVING PROCESS YOU MAY LOSE DATA)
    counter += 1
    if counter % 100 == 0:
        print("Paused for 5 seconds")
        time.sleep(1)
        print("Paused for 4 seconds")
        time.sleep(1)
        print("Paused for 3 seconds")
        time.sleep(1)
        print("Paused for 2 seconds")
        time.sleep(1)
        print("Paused for 1 seconds")
        time.sleep(1)
