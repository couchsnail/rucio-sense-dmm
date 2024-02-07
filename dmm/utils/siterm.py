import requests

def get_siterm_list_of_endpoints(site, certs):
    url = str(site.query_url) + "/MAIN/sitefe/json/frontend/configuration"
    data = requests.get(url, cert=certs, verify=False).json()
    return data[site.name]["metadata"]["xrootd"].items()

def ping():
    pass
    # data = {
    #         "type": "rapidping",
    #         "sitename": ,
    #         "hostname": ,
    #         "ip": "",
    #         "packetsize": "32",
    #         "interval": "1",
    #         "interface": "dummyinterface",
    #         "time": "60",
    #     }