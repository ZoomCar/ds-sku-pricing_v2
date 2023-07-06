import json
import requests

def slack_notification_trigger(web_hook_url,message,username):
    # Set the webhook_url to the one provided by Slack when you create the webhook at https://my.slack.com/services/new/incoming-webhook/
    message_string =str(message)
    slack_data = {'error': message_string,
                  'text': message_string,
                  'username': username,
                  'icon_emoji': ':robot_face:'
                  }
    response = requests.post(web_hook_url, data=json.dumps(slack_data), headers={'Content-Type': 'application/json'})
    if response.status_code != 200:
        raise ValueError (
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
        )