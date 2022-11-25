from utils.robot_utils import TextBlockTitleTeams, TextBlockTeams, MentionTeams
from utils.robot_utils import json_for_message, webhook_url
import requests
import json

HEADERS = {'Content-type': 'application/json', 'Accept': 'text/plain'}

def success_message(title, success_message, type):
    json_teams = json_for_message

    if isinstance(title, str):
        textBlock = TextBlockTitleTeams()
        textBlock.text = title
        json_teams['attachments'][0]['content']['body'].append(textBlock.__dict__)
    else:
        raise TypeError # Necessário que seja String

    if isinstance(success_message, str):
        textBlock = TextBlockTeams()
        textBlock.text = success_message
        json_teams['attachments'][0]['content']['body'].append(textBlock.__dict__)
    elif isinstance(success_message, list):
        for i in success_message:
            textBlock = TextBlockTeams()
            textBlock.text = i
            json_teams['attachments'][0]['content']['body'].append(textBlock.__dict__)
    else:
        raise TypeError # Necessário que seja String ou uma lista

    send_webhook(json_teams,type)

def warning_message():
    print('warning')

def error_message(title,mentions,error_message, type):
    json_teams = json_for_message

    if isinstance(title, str):
        textBlock = TextBlockTitleTeams()
        textBlock.text = title
        json_teams['attachments'][0]['content']['body'].append(textBlock.__dict__)
    else:
        raise TypeError # Necessário que seja String

    if isinstance(error_message, str):
        textBlock = TextBlockTeams()
        textBlock.text = error_message
        json_teams['attachments'][0]['content']['body'].append(textBlock.__dict__)
    elif isinstance(error_message, list):
        for i in error_message:
            textBlock = TextBlockTeams()
            textBlock.text = i
            json_teams['attachments'][0]['content']['body'].append(textBlock.__dict__)
    else:
        raise TypeError # Necessário que seja String ou uma lista

    if isinstance(mentions, str):
        textBlock = TextBlockTeams()
        textBlock.text = '<at>'+mentions.split("@")[0]+'</at>'
        mention = MentionTeams()
        mention.text = '<at>'+mentions.split("@")[0]+'</at>'
        mention.mentioned['name'] = mentions.split("@")[0]
        mention.mentioned['id'] = mentions
        json_teams['attachments'][0]['content']['body'].append(textBlock.__dict__)
        json_teams['attachments'][0]['content']['msteams']['entities'].append(mention.__dict__)
    elif isinstance(mentions, list):
        for i in mentions:
            textBlock = TextBlockTeams() 
            textBlock.text = '<at>'+i.split("@")[0]+'</at>'
            mention = MentionTeams()
            mention.text = '<at>'+i.split("@")[0]+'</at>'
            mention.mentioned['name'] = i.split("@")[0]
            mention.mentioned['id'] = i
            json_teams['attachments'][0]['content']['body'].append(textBlock.__dict__)
            json_teams['attachments'][0]['content']['msteams']['entities'].append(mention.__dict__)
    else:
        raise TypeError # Necessário que seja String ou uma lista

    send_webhook(json_teams, type)

def send_webhook(json_teams,type):
    json_teams = json.dumps(json_teams)
    print(json_teams)
    r = requests.post(url=webhook_url[type], data=json_teams, headers=HEADERS)
    print(r)
    print(r.text)