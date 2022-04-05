#!/usr/bin/env python
# coding: utf-8

# In[2]:


# Import rebill function for slack command

from RebillDataSlackFunction import rebillsdata

### Slack Reporting

# Import modules

import logging
import os
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

load_dotenv()

# Authenticate
slack_bot_token = 'xoxb-500343966052-3215468188451-1OhkEe0BqeW1BRlDbX869NTI'
auth_token = 'xapp-1-A036BDQ6T27-3239386706256-f80a783dff1615c69bd55231758675516d811e087ad733453d017851b06d2f17'

app = App(token=slack_bot_token, name = "rebills")
#logger = logging.getLogger(__name__)

# Rebills Data

@app.command('/rebills')
def rebills(ack, respond, say):
    ack()
    lst = rebillsdata() # from  RebillDatav3.py
    text = "*Rebill Information*" + "```" +                "\n"  + lst[0] +                                "\n"                                            "\n"  + lst[1] +                                "\n"                                            "\n"  + lst[2] +                                "\n"                                            "\n"  + lst[3] + "```"
    say(text)
    
if __name__ == "__main__":
    SocketModeHandler(app,auth_token).start()
