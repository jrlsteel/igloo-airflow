import requests
from requests_aws4auth import AWS4Auth

obj = {
    "RoleArn": "arn:aws:iam::555393537168:role/cdw-orch-AirflowMasterTaskRole",
    "AccessKeyId": "ASIAYCUAI2SIEN4NBI55",
    "SecretAccessKey": "0NIoIqALecyQmUF0hzOIUHsV7ZDzZ4RaMCh9KtnF",
    "Token": "IQoJb3JpZ2luX2VjENX//////////wEaCWV1LXdlc3QtMSJHMEUCIHuPjQhcrErkuKKe3/WPQihxlwhMHP2CpgpufysycmoKAiEAnVb2F/f8jXj3RT397ETvn8WLeT+0t9vEc4LVPXgiWVIq+QMILhABGgw1NTUzOTM1MzcxNjgiDBLm+nNoHyKpZ1EAZSrWA+MrFBfDQAHx7Rw2i2gxSJwjohb0C+wM1Bujskw86y7yFa4KoWKuu6icSuEK7cCJDsqWv8vr9/pD33Gl/tKoLw5+dccLLtUSyyaY/39FuG1B6LIGFFhZgw4Cm1KUzH4qei/yYbodvIpzNFoDjfX8SUiFgcoIzGkWArPXelV0Qvx7owINbygZRmpJ7HKaWVjfcm5kRosEDzZ8ASu7TUam74ZzLJhrlrBF2Pbaqf5QiTobs9SuLx9n9+i2JHdAMsEaRzs5WTmzmucXdwDm52ZXBoJyQsgt8P4vyyJoJGx0rwdGVd3nwbzvtAgk/LioxQDdSEdbFfgZ825lfzji0clDzBbGCcFhZnBLLlPRSfiqAvv9vQZW8iO0duKkty5/B97rb9do76t4PNDQJHJFZmJMfkdpxBMmukUKcoJEJQPvj7oDBd2rJEWvA3gUX3lHD0Ud/BrD0Lj/4i7pb/F9Fa7d39dhg2PG47HTi/dKdces6BAcKqJWcM4oeqSoQSxNC1KjC2w2FCorVIKnugj1gB2c2BZ8HgWAG1ywFcMi4KujcZqMZgG+cSnBJLDVrkbuC1wLgpBvhkmheT/i/OJyHtJyg3Iz1aJTq0iBPL2arZsPCpFrA9NqVM62MIiN/YkGOqUBV4NqGXQm99O98Gz+3on5eposGZlCr7hEV6BadNeVToA514JREzple4RQznHppIZH0HFvQQnGr5h/21QiVDPlgtUyQO+alAxATHzxA6xu+waZMtzczK1Ucd6YG3ijZV8HVmFwksCP2RvASzIC2nlH+b4EJap81Oz1XI72++idZAZLlEHrtCF4YobEfbXsKJCZpgXF/jXn68wb2ZOt37rdhxMnXC/G",
    "Expiration": "2021-09-13T18:39:36Z",
}

url = "https://hf9erhtu5i.execute-api.eu-west-1.amazonaws.com/prod/test"
accessId = "ASIAYCUAI2SIKGJXRLTZ"
accessKey = "WEeyh6Px9gdB3V9Ct4XYgzW55J9th3Jy6vBLqyLv"

access_key = obj["AccessKeyId"]
access_key_secret = obj["SecretAccessKey"]
session_token = obj["Token"]


auth = AWS4Auth(access_key, access_key_secret, "eu-west-1", "execute-api", session_token=session_token)
response = requests.post(
    url,
    json={
        "data": "something",
    },
    auth=auth,
)

print(response.json())
