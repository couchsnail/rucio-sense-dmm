import requests
import json

DMM_URL = "http://nrp-01.nrp-nautilus.io"
RULE_ID = "8f49ca05db0b42bda9a91c87c44f5c9c"

response = requests.get(DMM_URL + '/query/' + RULE_ID)

