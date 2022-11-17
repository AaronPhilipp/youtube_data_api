# YouTube Data API
An experiment on how much data can be collected via the YouTube Data API in PYTHON.

API Reference: https://developers.google.com/youtube/v3/docs

## Why using an API?

Through an API we can access informations directly without downloading big datasets and search for the informations we especially need. The API is the interface which allows us to communicate with third-parties.

## Understanding YouTube-specific-variables

There are different youtube-specific-variables you should know when working with the API. 

### channel ID

There are 3 different ways on how your channel URL is built up. Channel Id, Custom Name or Handle. To retrieve informations froma specific channel you should use the unique channel id which consists of numbers and letters and can look like this: `UCUZHFZ9jIKrLroW8LcyJEQQ`

Google Reference: https://support.google.com/youtube/answer/6180214#

### video ID

The video ID will be located at the end of the video URL, right after the `v=`.
