#webhook_url_test = "https://hdatasolucoes.webhook.office.com/webhookb2/dae4d8e5-1a9b-48f0-af9f-a12f4bc3fe7c@078722a8-5f8a-40ef-9cb2-99c85f74b373/IncomingWebhook/4908407a19d54075b313192c6271dabb/316eda54-40a9-49f5-a204-40ea6bd88eef"

#webhook_url = "https://hdatasolucoes.webhook.office.com/webhookb2/72f95ada-641f-4696-bab9-b9bb67fc7247@078722a8-5f8a-40ef-9cb2-99c85f74b373/IncomingWebhook/4cf7e5ad02194889846cacf58ff901b5/316eda54-40a9-49f5-a204-40ea6bd88eef"

webhook_url = {"Stage":"https://falconi365.webhook.office.com/webhookb2/dd3a93f8-f642-4c2e-a1d2-d3957e5ad0d0@0c0bcda4-1b8e-46ab-b56c-4ae3741f4340/IncomingWebhook/a513a7578e6a47c0928059eee6dac29c/61b99e5f-f7fb-408c-893c-401939002842"}


check_count_atendimento = {"@type": "MessageCard","@context": "http://schema.org/extensions","themeColor": "0076D7","summary": "Verificações","sections": [{"activityTitle": "{nome_rede}","activitySubtitle": "Rede Nº {cod_rede}","activityImage": "{link_logo}","facts": [{"name": "{quant_d_1}","value": "Atendimentos ontem {dia_da_semana}"}, {"name": "{quant_d_8}","value": "Atendimentos {dia_da_semana} retrasada"}, {"name": "Status","value": "{resultado}"}],"markdown": True}]}


#Estrutura para envio
json_for_message =  {"type":"message","attachments":[{"contentType": "application/vnd.microsoft.card.adaptive","content": {"type": "AdaptiveCard","body": [],"$schema": "http://adaptivecards.io/schemas/adaptive-card.json","version": "1.0","msteams": {"entities": []}}}]}


#Encaixar em Entities
#mentionTeams = {"type": "mention","text": "{name}","mentioned": {"id": "{mail}","name": "{name}"}}

class MentionTeams(object):
    def __init__(self):
        self.type = "mention"
        self.text = "Name"
        self.mentioned = Mentioned().__dict__

class Mentioned(object):
    def __init__(self):
        self.id = "Mail"
        self.name = "Name"

class TextBlockTeams(object):
    def __init__(self):
        self.type = "TextBlock"
        self.text = "Message"

class TextBlockTitleTeams(object):
    def __init__(self):
        self.type = "TextBlock"
        self.size = "Medium"
        self.weight = "Bolder"
        self.text = "Message"
    def do_nothing(self):
        pass



