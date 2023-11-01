import pandas as pd
import requests
import datetime
import time
import os
import csv

# KEY

# api_key = 'XXX'

# load the channels-IDs
channel_ids = pd.read_csv(
    filepath_or_buffer="XXX.csv",
    index_col=None,  # we have no index column in the data
    header=0,  # the first row contains the header of the columns
    sep=",",
    encoding="latin1",
    dtype={"channel_id": str},  # specify the type of the columns
)

# Make a list of the dataframe to pass it later to the for-loop
channel_ids_list = channel_ids["channel_id"].to_list()

# SPECIFYING THE PATH FOR SAVING
path = "XXX\\"


# FUNCTION TO GET ALL CHANNEL DATA
def get_channel_informations(channel_id_arg, api_key_arg):
    final_df = pd.DataFrame(
        columns=[
            "channel_id",
            "channel_title",
            "channel_description",
            "publication_date",
            "view_count",
            "subscriber_count",
            "video_count",
            "topic_categories",
            "keywords",
            "thumbnail_url",
            "timestamp",
        ]
    )

    url = (
        "https://youtube.googleapis.com/youtube/v3/channels?part=id&part=snippet&part=statistics&part="
        "topicDetails&part=contentDetails&part=brandingSettings&id="
        + channel_id_arg
        + "&maxResults=20&pageToken="
        + "None"
        + "&key="
        + api_key_arg
    )

    response = requests.get(url=url).json()
    time.sleep(0.01)  # short break after request

    if "error" in response:
        raise TypeError(response["error"]["message"])

    # Some channels don't exist anymore. We need this if-else-statement to not exit the script.
    if "items" in response:
        channel_id = response["items"][0]["id"]
        channel_title = response["items"][0]["snippet"]["title"]
        channel_description = response["items"][0]["snippet"]["description"]
        publication_date = response["items"][0]["snippet"]["publishedAt"]
        view_count = response["items"][0]["statistics"]["viewCount"]
        subs_count = response["items"][0]["statistics"]["subscriberCount"]
        video_count = response["items"][0]["statistics"]["videoCount"]
        if "topicDetails" not in response["items"][0]:
            topic_categories = ""
        elif "topicDetails" in response["items"][0]:
            if "topicCategories" not in response["items"][0]["topicDetails"]:
                topic_categories = ""
            elif "topicCategories" in response["items"][0]["topicDetails"]:
                topic_categories = response["items"][0]["topicDetails"][
                    "topicCategories"
                ]
        if "keywords" not in response["items"][0]["brandingSettings"]["channel"]:
            keywords = ""
        elif "keywords" in response["items"][0]["brandingSettings"]["channel"]:
            keywords = response["items"][0]["brandingSettings"]["channel"]["keywords"]
        thumbnail_url = response["items"][0]["snippet"]["thumbnails"]["high"]["url"]
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    elif "items" not in response:
        channel_id = channel_id_arg
        channel_title = ""
        channel_description = ""
        publication_date = ""
        view_count = ""
        subs_count = ""
        video_count = ""
        topic_categories = ""
        keywords = ""
        thumbnail_url = ""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    final_df = pd.concat(
        [
            final_df,
            pd.DataFrame(
                [
                    {
                        "channel_id": channel_id,
                        "channel_title": channel_title,
                        "channel_description": channel_description,
                        "publication_date": publication_date,
                        "view_count": view_count,
                        "subscriber_count": subs_count,
                        "video_count": video_count,
                        "topic_categories": topic_categories,
                        "keywords": keywords,
                        "thumbnail_url": thumbnail_url,
                        "timestamp": timestamp,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

    return final_df


# SAVE DATA

for channels in channel_ids_list:
    tmp = get_channel_informations(channel_id_arg=channels, api_key_arg=api_key)
    if os.path.isfile(
        path + "channel_data_" + time.strftime("%Y-%m-%d") + ".csv"
    ):
        df = pd.read_csv(
            filepath_or_buffer=path
            + "channel_data_"
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
                "channel_title": str,
                "channel_description": str,
                "publication_date": str,
                "subscriber_count": float,
                "video_count": float,
                "topic_categories": str,
                "keywords": str,
                "thumbnail_url": str,
                "timestamp": str,
            },
        )
        df = pd.concat([df, tmp], ignore_index=True)
        df.to_csv(
            path_or_buf=path
            + "channel_data_"
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
            + "channel_data_"
            + time.strftime("%Y-%m-%d")
            + ".csv",
            sep=",",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
            encoding="utf-8-sig",
            index=False,
        )

    # delete the channel_id from our data_input csv and overwrite the original
    channel_ids = channel_ids[channel_ids["channel_id"] != channels]
    channel_ids.to_csv("XXX.csv")
