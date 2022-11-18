from youtube_transcript_api import YouTubeTranscriptApi

text = []

video_id = 'tbapalw2-Eo'

caption = YouTubeTranscriptApi.get_transcript(video_id=video_id, languages=['de'])

for i in caption:
    outtxt = (i['text'])
    text.append(outtxt)

    with open('op.txt', "a") as opf:
        opf.write(outtxt + '\n')
