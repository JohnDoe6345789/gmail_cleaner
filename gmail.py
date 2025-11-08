from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os.path
import pickle

# Scopes - full Gmail access
SCOPES = ['https://mail.google.com/']

def get_service():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return build('gmail', 'v1', credentials=creds)

def delete_all_emails():
    service = get_service()
    user_id = 'me'
    batch_size = 500  # Gmail API max batch size

    while True:
        results = service.users().messages().list(userId=user_id, maxResults=batch_size).execute()
        messages = results.get('messages', [])
        if not messages:
            print("No more messages found.")
            break

        msg_ids = [m['id'] for m in messages]
        service.users().messages().batchDelete(userId=user_id, body={'ids': msg_ids}).execute()
        print(f"Deleted {len(msg_ids)} messages...")

    print("✅ All messages deleted!")

if __name__ == '__main__':
    delete_all_emails()
