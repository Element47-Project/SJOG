from exchangelib import Credentials, Account, Configuration, DELEGATE

# Replace these with your email credentials
EMAIL_ADDRESS = 'element47testing@outlook.com'
PASSWORD = 'reviveyourbody47'


def login_and_fetch_emails(email, password):
    # Set up credentials and account
    credentials = Credentials(email, password)
    account = Account(email, credentials=credentials, autodiscover=True, access_type=DELEGATE)

    # Fetch the first 10 items from the inbox
    inbox = account.inbox
    for item in inbox.all().order_by('-datetime_received')[:10]:
        print(item.subject, item.sender, item.datetime_received)


if __name__ == '__main__':
    login_and_fetch_emails(EMAIL_ADDRESS, PASSWORD)
