# Transcribe-Calls

This is the repo for the Transcriptions code in use by RMC Compliance team. The purpose of the code is to take the mp3 call recording files between RMC agents and customers, turn that into a text transcript, then highlight some key words and provide information for compliance to make the mandatory checks. The code leverages AWS Transcribe to do the actual transcription. A sample (redacted) is attached.

### Main Steps:
1. Make API call to convoso to get call recordings (mp3 file)
2. Upload the audio file to S3
3. Start the job in AWS Transcribe and return the raw json transcription when finished
4. Mark up and clean up the transcription, adding in extra customer information
