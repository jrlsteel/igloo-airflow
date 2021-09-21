import json


def get_secret(client, secretId):
    secret = client.get_secret_value(SecretId=secretId)
    secret_obj = json.loads(secret["SecretString"])
    return secret_obj
