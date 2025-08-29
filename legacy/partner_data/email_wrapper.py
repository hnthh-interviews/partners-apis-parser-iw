import imaplib
import email
import io
from ..config import IMAP_HOST, IMAP_PORT


class EmailWrapper():
    def __init__(self, username, password, imap_host=None, imap_port=None):
        mail = imaplib.IMAP4_SSL(imap_host if imap_host else IMAP_HOST, 
                                 imap_port if imap_port else IMAP_PORT)

        mail.login(username, password)
        status, messages = mail.select('INBOX')
        if status != "OK": raise Exception(f"Incorrect mail box {username}, {imap_host}:{imap_port}")
        self._messages = messages
        self._mail = mail

    def get_msg_w_attachments(self, sbj_contents):
        # for i in range(1, int(self._messages[0])+1, -1):
        for i in range(int(self._messages[0])+1, 1, -1):
            res, msg = self._mail.fetch(str(i), '(RFC822)')
            for response in msg:
                if isinstance(response, tuple):
                    msg = email.message_from_bytes(response[1])
                    try:
                        sbj = email.header.decode_header(msg["subject"])[0][0].decode()
                    except:
                        sbj = email.header.decode_header(msg["subject"])[0][0]
                    #sbj = msg["subject"]
                    print(f"{sbj}:{sbj_contents}:{sbj_contents in sbj}")
                    if sbj_contents in sbj: #'Fri, 26 May 2023 06:34:02 +0000 (UTC)'
                        sdt = msg['Date']
                        # date_obj = datetime.datetime.strptime(sdt, '%a, %d %b %Y %H:%M:%S %z')

                        for part in msg.walk():
                            # multipart/* are just containers
                            #if part.get_content_maintype() == 'multipart':
                            #    continue
                            if part.get_content_disposition() == 'attachment':
                                # Extract the filename and content type
                                filename = part.get_filename()
                                # Save the attachment to a file
                                if filename:
                                    f = io.BytesIO(part.get_payload(decode=True))
                                    if f is not None:
                                        f.name = filename
                                        f.mail_date = msg['Date']
                                        yield f

                                    # f = io.StringIO(part.get_payload(decode=True))
                                    # df = pd.read_excel(f, header=1)
                                    # print(df)
