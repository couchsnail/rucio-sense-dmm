import requests
from time import sleep
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

def get_siterm_list_of_endpoints(site, certs):
    url = str(site.query_url) + "/MAIN/sitefe/json/frontend/configuration"
    data = requests.get(url, cert=certs, verify=False).json()
    return data[site.name]["metadata"]["xrootd"].items()

def debugActions(site, dataIn, certs):
    # SUBMIT
    urls = str(site.query_url) + "/MAIN/sitefe/json/frontend/submitdebug/NEW"
    outs = requests.post(urls, data=dataIn, cert=certs, verify=False).json()
    print(outs)
    sleep(60)
    # GET
    urlg = str(site.query_url) + f"/MAIN/sitefe/json/frontend/getdebug/{outs.get('ID')}"
    outg = requests.get(urlg, cert=certs, verify=False).json()
    print(outg)
    # DELETE
    urld = str(site.query_url) + f"/MAIN/sitefe/json/frontend/deletedebug/{outs.get('ID')}"
    outd = requests.delete(urld, cert=certs, verify=False).json()
    print(outd)

def ping(site, certs):
    data_in = {'type': 'rapidping', 'sitename': "T2_US_Caltech_Test", 'hostname': 'sdn-dtn-1-7.ultralight.org',  
               'ip': "172.18.3.2", 'interface': 'vlan.3987', 'time': '5', "packetsize": "32"}
    debugActions(site, dataIn=data_in, certs=certs)

if __name__ == "__main__":
    class A:
        def __init__(self, query_url):
            self.query_url = query_url
    site = A("https://sense-caltech-fe.sdn-lb.ultralight.org:443")
    certs = ("/home/users/aaarora/private/certs/rucio-sense/cert.pem", "/home/users/aaarora/private/certs/rucio-sense/key.pem")
    ping(site, certs)