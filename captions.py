from youtube_transcript_api import YouTubeTranscriptApi
from main import video_id

text = []

caption = YouTubeTranscriptApi.get_transcript(video_id=video_id, languages=['de'])

for i in caption:
    tmp = (i['text'])

    with open(str(video_id) + '.txt', "a") as opf:
        opf.write(tmp + '\n')
