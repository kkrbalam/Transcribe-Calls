import requests
import os
from pathlib import Path
import boto3
import time
import json
import datetime as dt
import string
from datetime import datetime
from tabulate import tabulate
import pandas as pd
import pdfkit


today = datetime.today().strftime('%Y-%m-%d')


def get_lp_calls(date):
    """
    Fetches data from leadspedia on all transfers

    Args:
        date:
            YYYY-MM-DD of the date you want to search in LP 
            Start date and end date are same (possible TODO)
    
    Returns:
            A dataframe of many columns (most unneeded) for all transfers
            This dataframe includes lead information (from convoso)
            and call information (client, billable, etc)
    """

    url = "https://api.leadspedia.com/core/v2/inboundCalls/getSoldCalls.do?api_key=" \
          + lp_api_key + "&api_secret=" + lp_api_secret + "&" + "fromDate=" + \
          date + "&toDate=" + date + "start=&limit=1000"
    r = requests.get(url)
    d = r.json()
    data = d['response']['data']

    print("Got LP data")
    return data


def get_convoso_calls(date, testing=False):
    """
    Gets MP3 call recordings from convoso
    
    Makes API call to convoso to get call logs, including URL for downloads.
    Checks off OneDrive file path to see if the call has already been transcribed,
    and if so, skips over that call
    Downloads the MP3 files from URLs of all the new calls and stores them locally
    TODO jh: include manual and inbound calls
    
    Args:
        date:
            YYYY-MM-DD
        testing:
            Default is False.
            If True, only downloads the 3 most recent calls, which saves time
    
    Returns:
        List of all the new recordings' filenames that need to be transcribed
        These are of the form AgentName-call_id-leadPhoneNumber.mp3
    """

    # make the API call to convoso to get the call log + recordings
    onedrive_path = '/Users/jameshull/Documents/OneDrive/Compliance/' + date + '/'

    url = 'https://api.convoso.com/v1/log/retrieve'
    params = {
        'auth_token': convoso_auth_token,
        'status': 'SALE',
        'include_recordings': '1',
        'limit': '500'
    }

    r = requests.get(url, params)

    d = r.json()

    if d['success']:
        if int(d['data']['total_found']) > 500:
            print("Too many transfers! Need to go back and get more")

        data = d['data']['results']
        total_found = d['data']['total_found']

        print("Total calls: " + total_found)

        counter = 1
        new_recs = []

        if testing:
            data = data[0:3]
            print("Only doing " + str(len(data)) + " calls in testing.")

        # loop through data (dictionary) and any new recording filenames get stored in new_recs
        for call in data:
            agent = call['user'].replace(" ", "")
            call_id = call['id']
            phone_number = call['phone_number']
            filename = agent + '-' + call_id + '-' + phone_number
            filepath = filename + '.mp3'

            rec_url = call['recording'][0]['public_url']
            onedrive_filepath = onedrive_path + filename + '.mp3'

            if not Path(onedrive_filepath).exists():
                recording = requests.get(rec_url, allow_redirects=True)
                mp3_file = open(filepath, 'wb').write(recording.content)
                new_recs.append(filepath)

            counter += 1
            if counter % 10 == 0:
                print("Finished with " + str(counter) + " / " + total_found)

        print("Got all the mp3 files from convoso")

        return new_recs


def transcribe_calls(new_recs):
    """
    Uploads MP3s to AWS for Transcriptions
    
    Takes locally stored MP3s and uploads them to a bucket in S3
    Creates a transcription job in AWS with the recording name
    Unrelated TODO jh: Add custom vocabulary to AWS
    
    Args:
        new_recs: 
            List which gets returned by get_convoso_calls of all the new recordings
            that need to be transcribed. 
            Each element of the form AgentName-callID-leadPhoneNumber.mp3
    
    Returns:
        nothing
    """

    # setting AWS info
    s3_resource = boto3.resource('s3')
    transcribe = boto3.client('transcribe', region_name="us-east-2")
    bucket = 'convoso-xfer-recordings-for-qc'
    total = len(new_recs)
    print("Total Calls to transcribe: " + str(total))

    for rec in new_recs:

        # check file size due to weird convoso error
        file_size = os.stat(rec).st_size
        if file_size > 1000:
            s3_resource.meta.client.upload_file(rec, bucket, rec)
            time.sleep(2)

            try:
                transcribe.start_transcription_job(
                    TranscriptionJobName=rec,
                    Media={'MediaFileUri': "s3://" + bucket + "/" + rec},
                    LanguageCode='en-US',
                    Settings={'ShowSpeakerLabels': True,
                              'MaxSpeakerLabels': 3
                              })
            except Exception:
                pass


def get_transcriptions(new_recs, date, data_for_tables):
    """
    Gets the json of the transcription from AWS and marks it up, and moves them
    
    Checks the status of the transcription job in AWS.
    If completed or failed (not in progress), proceeds.
    Takes the JSON output of the transcription from AWS and passes
    it to mark_up_transcript to do the highlighting
    Then calls move_files to move / delete all files
    
    Args:
        new_recs: 
            List returned by get_convoso_calls of all the new recordings
            that were just transcribed.
            Each element of the form AgentName-callID-leadPhoneNumber.mp3
        date:
            YYYY-MM-DD
        data_for_tables:
            Dictionary of leadspedia data for leads.
            Returned by get_table_data

    Returns:
        nothing
    """

    counter = 1
    # setting AWS info
    transcribe = boto3.client('transcribe', region_name="us-east-2")
    for rec in new_recs:

        if counter % 5 == 0:
            print("Finished with " + str(counter) + " / " + str(len(new_recs)))
            counter += 1

        # check file size due to weird convoso error
        file_size = os.stat(rec).st_size
        if file_size > 1000:
            completed = False

            # getting the status of the job
            while not completed:
                status = transcribe.get_transcription_job(TranscriptionJobName=rec)
                result = status['TranscriptionJob']['TranscriptionJobStatus']
                if result in ['COMPLETED', 'FAILED']:
                    break
                time.sleep(10)

            # saving the json output from transcribe
            if result == "COMPLETED":
                json_url = status['TranscriptionJob']['Transcript']['TranscriptFileUri']
                json_dl = requests.get(json_url, allow_redirects=True)
                json_filepath = rec[:-4] + '.json'
                open(json_filepath, 'wb').write(json_dl.content)

                if rec in data_for_tables.keys():
                    mark_up_transcript(json_filepath, data_for_tables[rec])
                else:
                    mark_up_transcript(json_filepath, None)

                move_files(rec, date)
                print("Finished with " + rec)

            else:
                print(rec + " failed")

        else:
            os.remove(rec)


def get_table_data(lp_data, new_recs):
    """
    Creates table of lead info for compliance
    
    The table has all the lead info that the agent entered into convoso 
    during the call. Used to verify what the lead says actually is what the
    agent selected.
    TODO jh: Clean this up / make prettier
    TODO jh: Add tree removal 
    
    Args:
        lp_data:
            Dataframe of leadspedia data that gets returned by get_lp_calls
        new_recs:
            List of new recordings that have just been transcribed / marked up
            Each element of the form AgentName-callID-leadPhoneNumber.mp3
    
    Returns:
        Dictionary of lead information that is to be appended to the transcript
    """

    data_for_tables = {}
    for rec in new_recs:
        pn = str(rec)[-14:-4]

        for call in lp_data:
            if int(call['callFrom'][1:]) == int(pn):
                data_for_tables[rec] = {
                    'client': call['buyerName'],
                    'zip_code': call['lead']['zip_code'],
                    'state': call['lead']['fields']['state'],
                    'bill': call['lead']['fields']['avg_electric'],
                    'provider': call['lead']['fields']['utility_provider'],
                    'property_type': call['lead']['fields']['property_type'],
                    'bk': call['lead']['fields']['bk_fc'],
                    'lates': call['lead']['fields']['mtg_lates_solar'],
                    'credit': call['lead']['fields']['credit_score'],
                    'shade': call['lead']['fields']['shade_level'],
                    'comments': call['lead']['fields']['comments'],
                    'phone_number': call['callFrom'][1:],
                    'income': call['lead']['fields']['solar_income']
                }
    return data_for_tables


def mark_up_transcript(filename, dft):
    """
    Cleans up JSON for compliance
    
    
    Adds mark ups / highlighting to raw transcript based on defined rules
    Appends lead info table
    
    Args:
        filename:
            One element from the list new_recs of all the new recordings being processed
            Of form AgentName-callID-leadPhoneNumber.mp3
        dft: 
            Dictionary of lead info for this lead to be appended at bottom of transcript
            Returned by get_table_data
            
    Returns:
        nothing
     """

    filename = filename.split('.')[0]

    # Create an output html file
    with open(filename + '.html', 'w') as w:
        with open(filename + '.json') as f:

            data = json.loads(f.read())
            labels = data['results']['speaker_labels']['segments']
            speaker_start_times = {}

            for label in labels:
                for item in label['items']:
                    speaker_start_times[item['start_time']] = item['speaker_label']

            items = data['results']['items']
            lines = []
            line = ''
            call_time = 0
            speaker = 'null'
            i = 0

            # loop through all elements
            for item in items:
                i = i + 1
                content = item['alternatives'][0]['content']

                # if it's starting time
                if item.get('start_time'):
                    current_speaker = speaker_start_times[item['start_time']]

                # in AWS output, there are types as punctuation
                elif item['type'] == 'punctuation':
                    line = line + content

                # handle different speaker
                if current_speaker != speaker:
                    if speaker:
                        lines.append({'speaker': speaker, 'line': line, 'time': call_time})
                    line = content
                    speaker = current_speaker
                    call_time = item['start_time']

                elif item['type'] != 'punctuation':
                    line = line + ' ' + content
            lines.append({'speaker': speaker, 'line': line, 'time': call_time})

            # sort the results by the time
            sorted_lines = sorted(lines, key=lambda k: float(k['time']))

            # write into the .txt file
            for line_data in sorted_lines:
                line = '[' + str(dt.timedelta(seconds=int(round(float(line_data['time']))))) + '] ' + line_data.get(
                    'speaker') + ': ' + line_data.get('line') + '<br>'
                text = line.split()
                for i in range(len(text)):
                    this_word = text[i].translate(str.maketrans('', '', string.punctuation)).lower()
                    if this_word == "credit":
                        word = text[i]
                        text[i] = "<span style='background-color:blue; font-size: 20pt'>" + word + "</span>"

                    elif this_word in ["income", "working", "retired"]:
                        word = text[i]
                        text[i] = "<span style='background-color:chartreuse; font-size: 20pt'>" + word + "</span>"

                    elif this_word in ["electric", "bill", "discounts", "provider", "electricity"]:
                        word = text[i]
                        text[i] = "<span style='background-color:darkorange; font-size: 20pt'>" + word + "</span>"

                    elif this_word in ["homeowner", "address"] or \
                            (this_word == "home" and text[i + 1] == "owner") or \
                            (this_word == "owner" and text[i - 1] == "home"):
                        word = text[i]
                        text[i] = "<span style='background-color:hotpink; font-size: 20pt'>" + word + "</span>"

                    elif this_word in ["shade", "tree", "trim", "removal"]:
                        word = text[i]
                        text[i] = "<span style='background-color:darkseagreen; font-size: 20pt'>" + word + "</span>"

                    elif this_word in ["property", "mobile", "manufactured", "townhouse", "townhome"] or \
                            (this_word == "single" and text[i + 1] == "family") or \
                            (this_word == "family" and text[i - 1] == "single"):
                        word = text[i]
                        text[i] = "<span style='background-color:darkgray; font-size: 20pt'>" + word + "</span>"

                    elif this_word in ["roof", "shingles", "flat", "pitched"]:
                        word = text[i]
                        text[i] = "<span style='background-color:magenta; font-size: 20pt'>" + word + "</span>"

                    elif this_word in ["bankruptcy", "foreclosure", "lates", "late", "mortgage"]:
                        word = text[i]
                        text[i] = "<span style='background-color:sandybrown; font-size: 20pt'>" + word + "</span>"

                    elif this_word in ["free", "time", "quick", "minute"]:
                        word = text[i]
                        text[i] = "<span style='background-color:red; font-size: 20pt'>" + word + "</span>"

                    elif this_word in ["recorded", "google", "callback", "minute"]:
                        word = text[i]
                        text[i] = "<span style='background-color:skyblue; font-size: 20pt'>" + word + "</span>"

                        # .join() with lists
                separator = ' '
                formatted_text = (separator.join(text))
                w.write(formatted_text + '\n\n')

        # adding the lp data
        if dft is not None:
            df = pd.DataFrame.from_dict(dft, orient='index')
            tbl_html = (tabulate(df, tablefmt='html'))
            w.writelines(tbl_html)
        else:
            w.write('Lead info failed to populate. Sorry.')

        # print("finished")


# In[69]:


def move_files(filename, date):
    """
    Deletes and moves files to final destinations
    
    Args:
        filename:
            File name of one recording / json that was just completed
            Of form AgentName-callID-leadPhoneNumber.mp3
        date:
            YYYY-MM-DD 
        
    Returns:
        nothing
    """

    filename = filename[:-4]
    if not os.path.exists("/Users/jameshull/Documents/OneDrive/Compliance/" + date):
        os.mkdir("/Users/jameshull/Documents/OneDrive/Compliance/" + date)
    if filename in os.listdir("/Users/jameshull/Documents/GitHub/transcriptions/"):
        os.rename(filename + '.mp3', "/Users/jameshull/Documents/OneDrive/Compliance/" + date + "/" + filename + ".mp3")
    if (filename + '.json') in os.listdir("/Users/jameshull/Documents/GitHub/transcriptions/"):
        os.remove(filename + '.json')
    if (filename + '.html') in os.listdir("/Users/jameshull/Documents/GitHub/transcriptions/"):
        os.rename(filename + '.html',
                  "/Users/jameshull/Documents/OneDrive/Compliance/" + date + "/" + filename + ".html")

    pdfkit.from_file("/Users/jameshull/Documents/OneDrive/Compliance/" + date + "/" + filename + ".html",
                     "/Users/jameshull/Documents/OneDrive/Compliance/" + date + "/" + filename + ".pdf")
    os.remove("/Users/jameshull/Documents/OneDrive/Compliance/" + date + "/" + filename + ".html")


def main():
    leadspedia_data = get_lp_calls(today)
    new_recordings = get_convoso_calls(today)
    lead_data = get_table_data(leadspedia_data, new_recordings)
    transcribe_calls(new_recordings)
    get_transcriptions(new_recordings, today, lead_data)


if __name__ == '__main__':
    main()
