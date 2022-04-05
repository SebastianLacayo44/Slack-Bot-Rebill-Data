# Import rebill function for slack command

from RebillDataFunction import rebillsdata

### Slack Reporting

# Import modules

import logging
import os
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

load_dotenv()

# Authenticate
slack_bot_token = '*' # ommitted for security purposes
auth_token = '*'      # ommitted for security purposes

app = App(token=slack_bot_token, name = "rebills")
logger = logging.getLogger(__name__)

# Rebills Data

@app.command('/rebills')
def rebills(ack, respond, say):
    ack()
    lst = rebillsdata() # from  RebillData.py
    text = "*Rebill Information*" + "```" +                
           "\n" + lst[0] + "\n" "\n"  + 
           lst[1] +"\n""\n"  + lst[2] + 
           "\n"  "\n"  + lst[3] + "```"
    say(text)
    
if __name__ == "__main__":
    SocketModeHandler(app,auth_token).start()
