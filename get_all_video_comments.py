import csv
import os
import pandas as pd
import requests
import datetime
import time

# KEY
# api_key = 'XXX'

# load the video-IDs
video_ids = pd.read_csv(
    filepath_or_buffer="XXX.csv",
    index_col=None,  # we have no index column in the data
    header=0,  # the first row contains the header of the columns
    sep=",",
    encoding="latin1",
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
    },  # specify the type of the columns
)

# Make a list of the dataframe to pass it later to the for-loop
video_ids_list = video_ids["video_id"].to_list()

# SPECIFYING THE PATH FOR SAVING
path = "XXX\\"

# DEFINING THE FUNCTION
def get_all_video_comments(video_id_arg, api_key_arg):
    final_df = pd.DataFrame(
        columns=[
            "comment_counter",
            "comment_count_video",
            "video_id",
            "comment_id",
            "channel_id",
            "channel_name",
            "content",
            "content_enc",
            "profile_image",
            "like_count",
            "publication_date_comment",
            "replies_count",
            "type",
            "parent_id",
            "timestamp",
        ]
    )

    url_c = (
        "https://youtube.googleapis.com/youtube/v3/videos?part=snippet&part=id&part=statistics&part="
        "localizations&part=status&part=contentDetails&part=liveStreamingDetails&part=topicDetails&id="
        + video_id_arg
        + "&maxResults=50&key="
        + api_key_arg
    )

    response = requests.get(url=url_c).json()

    if "error" in response:
        if response["error"]["message"] == "The request is missing a valid API key.":
            raise TypeError(response["error"]["message"])
        else:
            raise ValueError(response["error"]["message"])

    elif "items" in response:
        if response["items"]:
            try:
                comments_count = int(response["items"][0]["statistics"]["commentCount"])
            except KeyError:
                comments_count = int(0)

            print(
                video_id_arg
                + ": "
                + str(comments_count)
                + " Kommentar/e ("
                + datetime.datetime.now().strftime("%H:%M:%S")
                + ")"
            )

            url = (
                "https://youtube.googleapis.com/youtube/v3/commentThreads?part=id&part=replies&part=snippet&"
                "maxResults=100&order=time&videoId="
                + video_id_arg
                + "&key="
                + api_key_arg
            )

            response = requests.get(url=url).json()

            time.sleep(0.01)  # short break after request

            if "nextPageToken" not in response:
                page_token = []
            elif "nextPageToken" in response:
                page_token = response["nextPageToken"]

            if "error" in response:
                if (
                    response["error"]["message"]
                    == "The request is missing a valid API key"
                ):
                    raise TypeError(response["error"]["message"])
                elif (
                    response["error"]["message"]
                    == 'The video identified by the <code><a href="/youtube/v3/docs/commentThreads/list#videoId">videoId</a></code> parameter has disabled comments.'
                ):
                    raise RuntimeError(response["error"]["message"])
                elif (
                    response["error"]["message"]
                    == "The API server failed to successfully process the request. While this can be a transient error, it usually indicates that the request's input is invalid. Check the structure of the <code>commentThread</code> resource in the request body to ensure that it is valid."
                ):
                    raise AttributeError(response["error"]["message"])
                elif (
                    response["error"]["message"]
                    == "One or more of the requested comment threads cannot be retrieved due to insufficient permissions. The request might not be properly authorized."
                ):
                    raise ArithmeticError(response["error"]["message"])
                else:
                    raise ValueError(response["error"]["message"])

            elif "items" in response:
                for res1 in response["items"]:
                    video_id = res1["snippet"]["videoId"]

                    comment_id = res1["snippet"]["topLevelComment"]["id"]

                    channel_name = res1["snippet"]["topLevelComment"]["snippet"][
                        "authorDisplayName"
                    ]

                    if (
                        "authorChannelId"
                        in res1["snippet"]["topLevelComment"]["snippet"]
                    ):
                        channel_id = res1["snippet"]["topLevelComment"]["snippet"][
                            "authorChannelId"
                        ]["value"]
                    elif (
                        "authorChannelId"
                        not in res1["snippet"]["topLevelComment"]["snippet"]
                    ):
                        channel_id = ""

                    content = res1["snippet"]["topLevelComment"]["snippet"][
                        "textOriginal"
                    ]

                    content_enc = content.encode("raw-unicode-escape")

                    profile_image = res1["snippet"]["topLevelComment"]["snippet"][
                        "authorProfileImageUrl"
                    ]

                    like_count = res1["snippet"]["topLevelComment"]["snippet"][
                        "likeCount"
                    ]

                    publication_date_comment = res1["snippet"]["topLevelComment"][
                        "snippet"
                    ]["publishedAt"]

                    replies_count = res1["snippet"]["totalReplyCount"]

                    ctype = "comment"

                    parent_id = ""

                    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    final_df = pd.concat(
                        [
                            final_df,
                            pd.DataFrame(
                                [
                                    {
                                        "comment_counter": len(final_df.index) + 1,
                                        "comment_count_video": comments_count,
                                        "video_id": video_id,
                                        "comment_id": comment_id,
                                        "channel_name": channel_name,
                                        "channel_id": channel_id,
                                        "content": content,
                                        "content_enc": content_enc,
                                        "profile_image": profile_image,
                                        "like_count": like_count,
                                        "publication_date_comment": publication_date_comment,
                                        "replies_count": replies_count,
                                        "type": ctype,
                                        "parent_id": parent_id,
                                        "timestamp": timestamp,
                                    }
                                ]
                            ),
                        ],
                        ignore_index=True,
                    )

                    if replies_count > 0:
                        url2 = (
                            "https://youtube.googleapis.com/youtube/v3/comments?part=snippet&part=id&maxResults="
                            "100&parentId=" + comment_id + "&key=" + api_key_arg
                        )

                        response2 = requests.get(url=url2).json()
                        time.sleep(0.01)  # short break after request

                        if "error" in response2:
                            raise ValueError(response2["error"]["message"])

                        elif "items" in response2:
                            for res2 in response2["items"]:
                                comment_id = res2["id"]

                                channel_name = res2["snippet"]["authorDisplayName"]

                                channel_id = res2["snippet"]["authorChannelId"]["value"]

                                content = res2["snippet"]["textDisplay"]

                                content_enc = content.encode("raw-unicode-escape")

                                profile_image = res2["snippet"]["authorProfileImageUrl"]

                                like_count = res2["snippet"]["likeCount"]

                                publication_date_comment = res2["snippet"][
                                    "publishedAt"
                                ]

                                parent_id = res2["snippet"]["parentId"]

                                replies_count = ""

                                ctype = "reply"

                                timestamp = datetime.datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                )

                                final_df = pd.concat(
                                    [
                                        final_df,
                                        pd.DataFrame(
                                            [
                                                {
                                                    "comment_counter": len(
                                                        final_df.index
                                                    )
                                                    + 1,
                                                    "comment_count_video": comments_count,
                                                    "video_id": video_id,
                                                    "comment_id": comment_id,
                                                    "channel_name": channel_name,
                                                    "channel_id": channel_id,
                                                    "content": content,
                                                    "content_enc": content_enc,
                                                    "profile_image": profile_image,
                                                    "like_count": like_count,
                                                    "publication_date_comment": publication_date_comment,
                                                    "replies_count": replies_count,
                                                    "type": ctype,
                                                    "parent_id": parent_id,
                                                    "timestamp": timestamp,
                                                }
                                            ]
                                        ),
                                    ],
                                    ignore_index=True,
                                )

                while page_token != []:
                    url = (
                        "https://youtube.googleapis.com/youtube/v3/commentThreads?part=id&part=replies&part=snippet&maxResults=100&order=time&videoId="
                        + video_id_arg
                        + "&key="
                        + api_key_arg
                        + "&pageToken="
                        + page_token
                    )

                    response = requests.get(url=url).json()
                    time.sleep(0.01)  # short break after request

                    if "error" in response:
                        raise ValueError(response["error"]["message"])

                    elif "items" in response:
                        # If there are more than 50 results, the response gives back a nextPageToken to get the other results, so we need to save this Token
                        if "nextPageToken" not in response:
                            page_token = []
                        elif "nextPageToken" in response:
                            page_token = response["nextPageToken"]

                        for res3 in response["items"]:
                            video_id = res3["snippet"]["videoId"]

                            comment_id = res3["snippet"]["topLevelComment"]["id"]

                            channel_name = res3["snippet"]["topLevelComment"][
                                "snippet"
                            ]["authorDisplayName"]
                            if (
                                "authorChannelId"
                                in res3["snippet"]["topLevelComment"]["snippet"]
                            ):
                                channel_id = res3["snippet"]["topLevelComment"][
                                    "snippet"
                                ]["authorChannelId"]["value"]
                            elif (
                                "authorChannelId"
                                not in res3["snippet"]["topLevelComment"]["snippet"]
                            ):
                                channel_id = ""

                            content = res3["snippet"]["topLevelComment"]["snippet"][
                                "textOriginal"
                            ]

                            content_enc = content.encode("raw-unicode-escape")

                            profile_image = res3["snippet"]["topLevelComment"][
                                "snippet"
                            ]["authorProfileImageUrl"]

                            like_count = res3["snippet"]["topLevelComment"]["snippet"][
                                "likeCount"
                            ]

                            publication_date_comment = res3["snippet"][
                                "topLevelComment"
                            ]["snippet"]["publishedAt"]

                            replies_count = res3["snippet"]["totalReplyCount"]

                            ctype = "comment"

                            parent_id = ""

                            timestamp = datetime.datetime.now().strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )

                            final_df = pd.concat(
                                [
                                    final_df,
                                    pd.DataFrame(
                                        [
                                            {
                                                "comment_counter": len(final_df.index)
                                                + 1,
                                                "comment_count_video": comments_count,
                                                "video_id": video_id,
                                                "comment_id": comment_id,
                                                "channel_name": channel_name,
                                                "channel_id": channel_id,
                                                "content": content,
                                                "content_enc": content_enc,
                                                "profile_image": profile_image,
                                                "like_count": like_count,
                                                "publication_date_comment": publication_date_comment,
                                                "replies_count": replies_count,
                                                "type": ctype,
                                                "parent_id": parent_id,
                                                "timestamp": timestamp,
                                            }
                                        ]
                                    ),
                                ],
                                ignore_index=True,
                            )

                            if replies_count > 0:
                                url2 = (
                                    "https://youtube.googleapis.com/youtube/v3/comments?part=snippet&part=id&"
                                    "maxResults=100&parentId="
                                    + comment_id
                                    + "&key="
                                    + api_key_arg
                                )

                                response2 = requests.get(url=url2).json()
                                time.sleep(0.01)  # short break after request

                                if "error" in response2:
                                    raise ValueError(response2["error"]["message"])

                                elif "items" in response2:
                                    for res4 in response2["items"]:
                                        comment_id = res4["id"]

                                        channel_name = res4["snippet"][
                                            "authorDisplayName"
                                        ]

                                        channel_id = res4["snippet"]["authorChannelId"][
                                            "value"
                                        ]

                                        content = res4["snippet"]["textDisplay"]
                                        content_enc = content.encode(
                                            "raw-unicode-escape"
                                        )

                                        profile_image = res4["snippet"][
                                            "authorProfileImageUrl"
                                        ]

                                        like_count = res4["snippet"]["likeCount"]

                                        publication_date_comment = res4["snippet"][
                                            "publishedAt"
                                        ]

                                        parent_id = res4["snippet"]["parentId"]

                                        replies_count = ""

                                        ctype = "reply"

                                        timestamp = datetime.datetime.now().strftime(
                                            "%Y-%m-%d %H:%M:%S"
                                        )

                                        final_df = pd.concat(
                                            [
                                                final_df,
                                                pd.DataFrame(
                                                    [
                                                        {
                                                            "comment_counter": len(
                                                                final_df.index
                                                            )
                                                            + 1,
                                                            "comment_count_video": comments_count,
                                                            "video_id": video_id,
                                                            "comment_id": comment_id,
                                                            "channel_name": channel_name,
                                                            "channel_id": channel_id,
                                                            "content": content,
                                                            "content_enc": content_enc,
                                                            "profile_image": profile_image,
                                                            "like_count": like_count,
                                                            "publication_date_comment": publication_date_comment,
                                                            "replies_count": replies_count,
                                                            "type": ctype,
                                                            "parent_id": parent_id,
                                                            "timestamp": timestamp,
                                                        }
                                                    ]
                                                ),
                                            ],
                                            ignore_index=True,
                                        )
        else:
            raise TypeError("Items Error")

    return final_df


# SAVE DATA

# initial counter to pause the script for 5 seconds every 100 videos
counter = 0

for video in video_ids_list:
    try:
        tmp = get_all_video_comments(video_id_arg=video, api_key_arg=api_key)
        print(video + ": ok (" + datetime.datetime.now().strftime("%H:%M:%S") + ")")
        if os.path.isfile(
            path + "comment_data_" + time.strftime("%Y-%m-%d") + ".csv"
        ):
            df = pd.read_csv(
                filepath_or_buffer=path
                + "comment_data_"
                + time.strftime("%Y-%m-%d")
                + ".csv",
                delimiter=",",
                encoding="utf-8-sig",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                doublequote=True,
                index_col=None,
                dtype={
                    "comment_counter": float,
                    "comment_count_video": float,
                    "video_id": str,
                    "comment_id": str,
                    "channel_name": str,
                    "channel_id": str,
                    "content": str,
                    "content_enc": str,
                    "profile_image": str,
                    "like_count": float,
                    "publication_date_comment": str,
                    "replies_count": str,
                    "type": str,
                    "parent_id": str,
                    "timestamp": str,
                },
            )
            df = pd.concat([df, tmp], ignore_index=True)
            df.to_csv(
                path_or_buf=path
                + "comment_data_"
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
                + "comment_data_"
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
            video + ": typeerror (" + datetime.datetime.now().strftime("%H:%M:%S") + ")"
        )
        if os.path.isfile(path + "video_ids_typeerror.csv"):
            df = pd.read_csv(
                filepath_or_buffer=path + "video_ids_errors.csv",
                delimiter=",",
                encoding="utf-8-sig",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                doublequote=True,
                index_col=None,
                dtype={"video_id": str, "timestamp": str},
            )

            tmp = pd.DataFrame(
                [
                    {
                        "video_id": video,
                        "timestamp": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                ]
            )

            df = pd.concat([df, tmp], ignore_index=True)
            df.to_csv(
                path_or_buf=path + "video_ids_errors.csv",
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
                        "video_id": video,
                        "timestamp": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                ]
            )
            tmp.to_csv(
                path_or_buf=path + "video_ids_errors.csv",
                sep=",",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                encoding="utf-8-sig",
                index=False,
            )
    except RuntimeError:
        print(
            video
            + ": comments disabled ("
            + datetime.datetime.now().strftime("%H:%M:%S")
            + ")"
        )
        if os.path.isfile(path + "video_ids_comments_disabled.csv"):
            df = pd.read_csv(
                filepath_or_buffer=path
                + "video_ids_comments_disabled.csv",
                delimiter=",",
                encoding="utf-8-sig",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                doublequote=True,
                index_col=None,
                dtype={"video_id": str, "timestamp": str},
            )

            tmp = pd.DataFrame(
                [
                    {
                        "video_id": video,
                        "timestamp": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                ]
            )

            df = pd.concat([df, tmp], ignore_index=True)
            df.to_csv(
                path_or_buf=path + "video_ids_comments_disabled.csv",
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
                        "video_id": video,
                        "timestamp": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                ]
            )
            tmp.to_csv(
                path_or_buf=path + "video_ids_comments_disabled.csv",
                sep=",",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                encoding="utf-8-sig",
                index=False,
            )
    except AttributeError:
        print(
            video
            + ": comments livestream ("
            + datetime.datetime.now().strftime("%H:%M:%S")
            + ")"
        )
        if os.path.isfile(path + "video_ids_comments_livestream.csv"):
            df = pd.read_csv(
                filepath_or_buffer=path
                + "video_ids_comments_livestream.csv",
                delimiter=",",
                encoding="utf-8-sig",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                doublequote=True,
                index_col=None,
                dtype={"video_id": str, "timestamp": str},
            )

            tmp = pd.DataFrame(
                [
                    {
                        "video_id": video,
                        "timestamp": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                ]
            )

            df = pd.concat([df, tmp], ignore_index=True)
            df.to_csv(
                path_or_buf=path + "video_ids_comments_livestream.csv",
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
                        "video_id": video,
                        "timestamp": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                ]
            )
            tmp.to_csv(
                path_or_buf=path + "video_ids_comments_livestream.csv",
                sep=",",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                encoding="utf-8-sig",
                index=False,
            )
    except ArithmeticError:
        print(
            video
            + ": comments exclusive ("
            + datetime.datetime.now().strftime("%H:%M:%S")
            + ")"
        )
        if os.path.isfile(path + "video_ids_comments_exclusive.csv"):
            df = pd.read_csv(
                filepath_or_buffer=path
                + "video_ids_comments_exclusive.csv",
                delimiter=",",
                encoding="utf-8-sig",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                doublequote=True,
                index_col=None,
                dtype={"video_id": str, "timestamp": str},
            )

            tmp = pd.DataFrame(
                [
                    {
                        "video_id": video,
                        "timestamp": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                ]
            )

            df = pd.concat([df, tmp], ignore_index=True)
            df.to_csv(
                path_or_buf=path + "video_ids_comments_exclusive.csv",
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
                        "video_id": video,
                        "timestamp": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                ]
            )
            tmp.to_csv(
                path_or_buf=path + "video_ids_comments_exclusive.csv",
                sep=",",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                encoding="utf-8-sig",
                index=False,
            )

    # delete the video_id from our data_input csv and overwrite the original
    video_ids = video_ids[video_ids["video_id"] != video]
    print(str(len(video_ids)) + " videos remaining")
    video_ids.to_csv(
        path_or_buf="XXX.csv",
        sep=",",
        quotechar='"',
        quoting=csv.QUOTE_ALL,
        encoding="latin1",
        index=False,
    )

    # every 100 videos pause the script for 5 seconds
    counter += 1
    print(str(100 - counter) + " videos till Pause.")
    if counter % 100 == 0:
        print("Paused for 5 seconds to exit the script.")
        time.sleep(1)
        print("Paused for 4 seconds")
        time.sleep(1)
        print("Paused for 3 seconds")
        time.sleep(1)
        print("Paused for 2 seconds")
        time.sleep(1)
        print("Paused for 1 seconds")
        time.sleep(1)

        counter = 0
