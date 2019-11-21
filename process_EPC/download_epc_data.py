import requests
import os
import shutil

def download_epc_zip():
   email = 'tolulope.adegunle@igloo.energy'
   token = '262b855e8fb47003e8e34e036fcd800b7c767180'
   site_url = 'https://epc.opendatacommunities.org/login-with-token?token=' + token + '&email=' + email
   file_url = "https://epc.opendatacommunities.org/files/all-domestic-certificates.zip"
   # OPEN SESSION
   s = requests.Session()
   s.get(site_url)
   s.post(site_url)
   with open("all-domestic-certificates.zip",'wb') as file:
       r = s.get(file_url,stream=True,timeout=3600)
       for chunk in r.iter_content(chunk_size=1024):
           if chunk:
               file.write(chunk)
               file.flush()


if __name__ == '__main__':

    download_epc_zip()