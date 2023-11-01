import os
import pandas as pd
import requests
import datetime
import time
import csv

# KEYS
api_key = "XXX" 

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
path = "data\\"


def get_all_channel_videos(channel_id, api_key):
    df = pd.DataFrame(
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

    # check how much videos are uploaded by a channel because of quota restrictions:
    url = (
        "https://youtube.googleapis.com/youtube/v3/channels?part=id&part=snippet&part=statistics&part="
        "topicDetails&part=contentDetails&part=brandingSettings&id="
        + channel_id
        + "&maxResults=20&pageToken="
        + "None"
        + "&key="
        + api_key
    )

    response = requests.get(url=url).json()

    if "error" in response:
        raise ValueError(response["error"]["message"])

    elif "items" in response:
        video_count = int(response["items"][0]["statistics"]["videoCount"])

        print(
            channel_id
            + ": "
            + str(video_count)
            + " Videos ("
            + datetime.datetime.now().strftime("%H:%M:%S")
            + ")"
        )

        if video_count > 8000:
            raise RuntimeError(
                "The Channel uploaded more than 6000 Videos. The Channel ID is stored in a separate"
                " .csv to not exceed the quota limit."
            )
        elif video_count == 0:
            raise RuntimeError("The Channel has no videos uploaded.")

    # all uploads of a channel are stored in the so-called upload_playlist.
    # To get the link to this playlist you only need to replace the 'C' in the 2nd place with 'U'.
    playlist_id = channel_id[:1] + "U" + channel_id[2:]

    url = (
        "https://youtube.googleapis.com/youtube/v3/playlistItems?part=id&part=snippet&"
        "part=status&playlistId=" + playlist_id + "&maxResults=50&key=" + api_key
    )
    response = requests.get(url=url).json()
    time.sleep(0.01)  # short break after request

    if "error" in response:
        if (
            response["error"]["message"]
            == "The playlist identified with the request's "
            "<code>playlistId</code> parameter cannot be found."
        ):
            raise TypeError(response["error"]["message"])
        else:
            raise ValueError(response["error"]["message"])

    elif "items" in response:
        # If there are more than 50 results, the response gives back a nextPageToken to get the other results,
        # so we need to save this Token.
        if "nextPageToken" not in response:
            page_token = ""
        elif "nextPageToken" in response:
            page_token = response["nextPageToken"]

        # going through each item on the first page
        for i in response["items"]:
            channel_id = i["snippet"]["channelId"]
            video_id = i["snippet"]["resourceId"]["videoId"]
            publication_date = i["snippet"]["publishedAt"]
            video_title = i["snippet"]["title"]
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            url2 = (
                "https://youtube.googleapis.com/youtube/v3/videos?part=snippet&part=id&part=statistics&part="
                "localizations&part=status&part=contentDetails&part=liveStreamingDetails&part=topicDetails&id="
                + video_id
                + "&maxResults=50&key="
                + api_key
            )
            response2 = requests.get(url=url2).json()
            time.sleep(0.01)  # short break after request

            if "error" in response2:
                # if response2['error']['message'] == 'The playlist identified with the request\'s ' \
                #                                    '<code>playlistId</code> parameter cannot be found.':
                #     raise TypeError(response2['error']['message'])
                # else:
                raise ValueError(response2["error"]["message"])

            elif "items" in response2:
                video_description = response2["items"][0]["snippet"]["description"]

                # sometimes there is no thumbnail in maxres available. If this is the case,
                # we need to look for other resolutions
                if "maxres" in response2["items"][0]["snippet"]["thumbnails"]:
                    video_thumbnail = response2["items"][0]["snippet"]["thumbnails"][
                        "maxres"
                    ]["url"]
                elif "high" in response2["items"][0]["snippet"]["thumbnails"]:
                    video_thumbnail = response2["items"][0]["snippet"]["thumbnails"][
                        "high"
                    ]["url"]
                elif "medium" in response2["items"][0]["snippet"]["thumbnails"]:
                    video_thumbnail = response2["items"][0]["snippet"]["thumbnails"][
                        "medium"
                    ]["url"]
                else:
                    video_thumbnail = response2["items"][0]["snippet"]["thumbnails"][
                        "default"
                    ]["url"]

                try:
                    view_count = response2["items"][0]["statistics"]["viewCount"]
                except KeyError:
                    view_count = ""

                try:
                    like_count = response2["items"][0]["statistics"]["likeCount"]
                except KeyError:
                    like_count = ""

                try:
                    fav_count = response2["items"][0]["statistics"]["favoriteCount"]
                except KeyError:
                    fav_count = ""

                try:
                    comments_count = response2["items"][0]["statistics"]["commentCount"]
                except KeyError:
                    comments_count = ""

                try:
                    vid_duration = response2["items"][0]["contentDetails"]["duration"]
                except KeyError:
                    vid_duration = ""

                try:
                    video_tags = response2["items"][0]["snippet"]["tags"]
                except KeyError:
                    video_tags = ""

                livebroadcastcontent = response2["items"][0]["snippet"][
                    "liveBroadcastContent"
                ]

                df = pd.concat(
                    [
                        df,
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

        while page_token != "":
            url = (
                "https://youtube.googleapis.com/youtube/v3/playlistItems?part=id&part=snippet&part="
                "status&playlistId="
                + playlist_id
                + "&maxResults=50&key="
                + api_key
                + "&pageToken="
                + page_token
            )
            response = requests.get(url=url).json()
            time.sleep(0.01)  # short break after request

            if "nextPageToken" not in response:
                page_token = ""
            elif "nextPageToken" in response:
                page_token = response["nextPageToken"]

            if "error" in response:
                if (
                    response["error"]["message"]
                    == "The playlist identified with the request's "
                    "<code>playlistId</code> parameter cannot be found."
                ):
                    raise TypeError(response["error"]["message"])
                else:
                    raise ValueError(response["error"]["message"])

            elif "items" in response:
                for i in response["items"]:
                    # add a try/except statement if a KeyError occurs: We need this so the script doesn't exit
                    # try:
                    channel_id = i["snippet"]["channelId"]
                    # except KeyError:
                    # channel_id = ''

                    # try:
                    video_id = i["snippet"]["resourceId"]["videoId"]
                    # except KeyError:
                    # video_id = ''

                    # try:
                    publication_date = i["snippet"]["publishedAt"]
                    # except KeyError:
                    # publication_date = ''

                    # try:
                    video_title = i["snippet"]["title"]
                    # except KeyError:
                    # video_title = ''

                    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    url2 = (
                        "https://youtube.googleapis.com/youtube/v3/videos?part=snippet&part=id&part="
                        "statistics&part=localizations&part=status&part=contentDetails&part="
                        "liveStreamingDetails&part=topicDetails&id="
                        + video_id
                        + "&maxResults=50&key="
                        + api_key
                    )
                    response2 = requests.get(url=url2).json()
                    time.sleep(0.01)  # short break after request

                    if "error" in response2:
                        # if response2['error']['message'] == 'The playlist identified with the request\'s ' \
                        #                                    '<code>playlistId</code> parameter cannot be found.':
                        #     raise TypeError(response2['error']['message'])
                        # else:
                        raise ValueError(response2["error"]["message"])

                    elif "items" in response2:
                        try:
                            video_description = response2["items"][0]["snippet"][
                                "description"
                            ]
                        except KeyError:
                            video_description = ""

                        # sometimes there is no thumbnail in maxres available. If this is the case,
                        # we need to look for other resolutions
                        if "maxres" in response2["items"][0]["snippet"]["thumbnails"]:
                            video_thumbnail = response2["items"][0]["snippet"][
                                "thumbnails"
                            ]["maxres"]["url"]
                        elif "high" in response2["items"][0]["snippet"]["thumbnails"]:
                            video_thumbnail = response2["items"][0]["snippet"][
                                "thumbnails"
                            ]["high"]["url"]
                        elif "medium" in response2["items"][0]["snippet"]["thumbnails"]:
                            video_thumbnail = response2["items"][0]["snippet"][
                                "thumbnails"
                            ]["medium"]["url"]
                        else:
                            video_thumbnail = response2["items"][0]["snippet"][
                                "thumbnails"
                            ]["default"]["url"]

                        try:
                            view_count = response2["items"][0]["statistics"][
                                "viewCount"
                            ]
                        except KeyError:
                            view_count = ""

                        try:
                            like_count = response2["items"][0]["statistics"][
                                "likeCount"
                            ]
                        except KeyError:
                            like_count = ""

                        try:
                            fav_count = response2["items"][0]["statistics"][
                                "favoriteCount"
                            ]
                        except KeyError:
                            fav_count = ""

                        try:
                            comments_count = response2["items"][0]["statistics"][
                                "commentCount"
                            ]
                        except KeyError:
                            comments_count = ""

                        try:
                            vid_duration = response2["items"][0]["contentDetails"][
                                "duration"
                            ]
                        except KeyError:
                            vid_duration = ""

                        try:
                            video_tags = response2["items"][0]["snippet"]["tags"]
                            # TODO: Maybe there's a better formation than the original brackets.
                        except KeyError:
                            video_tags = ""

                        livebroadcastcontent = response2["items"][0]["snippet"][
                            "liveBroadcastContent"
                        ]

                        df = pd.concat(
                            [
                                df,
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
    return df


# GET VIDEO INFORMATION OF A SINGLE CHANNEL

# channel_id = 'XXX'
# df = get_all_channel_videos(channel_id=channel_id, api_key=api_key)
# df.to_csv((path + 'XXX.csv'), encoding='utf-8-sig')


# GET VIDEO INFORMATIONS OF MULTIPLE CHANNELS

for i in channel_ids_list:
    try:
        tmp = get_all_channel_videos(channel_id=i, api_key=api_key)
        print(i + ": ok" + " (" + datetime.datetime.now().strftime("%H:%M:%S") + ")")
        if os.path.isfile(
            path + "video_list_" + time.strftime("%Y-%m-%d") + ".csv"
        ):
            df = pd.read_csv(
                filepath_or_buffer=path
                + "video_list_"
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
                    "publication_date": str,
                    "video_description": str,
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
                + "video_list_"
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
                + "video_list_"
                + time.strftime("%Y-%m-%d")
                + ".csv",
                sep=",",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                encoding="utf-8-sig",
                index=False,
            )
    except RuntimeError:
        print(
            i + ": runtimeerror (" + datetime.datetime.now().strftime("%H:%M:%S") + ")"
        )
        if os.path.isfile(path + "channel_ids_over_9000.csv"):
            df = pd.read_csv(
                filepath_or_buffer=path + "channel_ids_over_9000.csv",
                delimiter=",",
                encoding="utf-8-sig",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                doublequote=True,
                index_col=None,
                dtype={"channel_id": str, "timestamp": str},
            )

            tmp = pd.DataFrame(
                [
                    {
                        "channel_id": i,
                        "timestamp": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                ]
            )

            df = pd.concat([df, tmp], ignore_index=True)
            df.to_csv(
                path_or_buf=path + "channel_ids_over_9000.csv",
                sep=",",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                encoding="utf-8-sig",
                index=False,
            )

        else:
            tmp = pd.DataFrame(
                [
                    {
                        "channel_id": i,
                        "timestamp": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                ]
            )

            tmp.to_csv(
                path_or_buf=path + "channel_ids_over_9000.csv",
                sep=",",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                encoding="utf-8-sig",
                index=False,
            )
    except TypeError:
        print(i + ": typeerror (" + datetime.datetime.now().strftime("%H:%M:%S") + ")")
        if os.path.isfile(path + "channel_ids_errors.csv"):
            df = pd.read_csv(
                filepath_or_buffer=path + "channel_ids_errors.csv",
                delimiter=",",
                encoding="utf-8-sig",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                doublequote=True,
                index_col=None,
                dtype={"channel_id": str, "timestamp": str},
            )

            tmp = pd.DataFrame(
                [
                    {
                        "channel_id": i,
                        "timestamp": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                ]
            )

            df = pd.concat([df, tmp], ignore_index=True)
            df.to_csv(
                path_or_buf=path + "channel_ids_errors.csv",
                sep=",",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                encoding="utf-8-sig",
                index=False,
            )
        else:
            tmp = pd.DataFrame(
                [
                    {
                        "channel_id": i,
                        "timestamp": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                ]
            )

            tmp.to_csv(
                path_or_buf=path + "channel_ids_errors.csv",
                sep=",",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                encoding="utf-8-sig",
                index=False,
            )
    except IndexError:
        print(i + ": indexerror (" + datetime.datetime.now().strftime("%H:%M:%S") + ")")
        if os.path.isfile(path + "channel_ids_indexerrors.csv"):
            df = pd.read_csv(
                filepath_or_buffer=path + "channel_ids_indexerrors.csv",
                delimiter=",",
                encoding="utf-8-sig",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                doublequote=True,
                index_col=None,
                dtype={"channel_id": str, "timestamp": str},
            )

            tmp = pd.DataFrame(
                [
                    {
                        "channel_id": i,
                        "timestamp": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                ]
            )

            df = pd.concat([df, tmp], ignore_index=True)
            df.to_csv(
                path_or_buf=path + "channel_ids_indexerrors.csv",
                sep=",",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                encoding="utf-8-sig",
                index=False,
            )
        else:
            tmp = pd.DataFrame(
                [
                    {
                        "channel_id": i,
                        "timestamp": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                ]
            )

            tmp.to_csv(
                path_or_buf=path + "channel_ids_indexerrors.csv",
                sep=",",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                encoding="utf-8-sig",
                index=False,
            )

    channel_ids = channel_ids[channel_ids["channel_id"] != i]
    print(str(len(channel_ids)) + " channels remaining")
    channel_ids.to_csv(
        path_or_buf="XXX.csv",
        sep=",",
        quotechar='"',
        quoting=csv.QUOTE_ALL,
        encoding="latin1",
        index=False,
    )
