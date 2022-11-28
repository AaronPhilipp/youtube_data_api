from youtube_transcript_api import YouTubeTranscriptApi
from main import video_id
import pandas as pd
from main import path

def get_caption(video_id):
    df = pd.DataFrame(columns=['video_id', 'subtitle'])

    transcript_list = YouTubeTranscriptApi.list_transcripts(video_id=video_id)
    transcript = transcript_list.find_generated_transcript(['de', 'en'])

    if transcript == 'de ("Deutsch (automatisch erzeugt)")[TRANSLATABLE]':

        result = transcript.fetch()

        text = ''

        for i in result:
            text += i['text'] + ' '

    elif 'en ("Englisch (automatisch erzeugt)")[TRANSLATABLE]':

        translated_transcript = transcript.translate('de')
        result = translated_transcript.fetch()

        text = ''

        for i in result:
            text += i['text'] + ' '

    df = pd.concat([df, pd.DataFrame([{'video_id': video_id, 'subtitle': text}])])

    return df



# GET SUBTITLE OF MULTIPLE VIDEOS

big = pd.DataFrame()
video_ids = ['NqI6PMRxT0Y', 'R277Tc35Y4A', '-E-Qe8jdbbQ']

for i in video_ids:
    df = get_caption(video_id=i)
    big = pd.concat([big, df], ignore_index=True)
    big.to_csv((path + 'XXX.csv'),  encoding='utf-8-sig')
