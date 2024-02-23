import requests
from time import sleep
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

def get_siterm_list_of_endpoints(site, certs):
    url = str(site.query_url) + "/MAIN/sitefe/json/frontend/configuration"
    data = requests.get(url, cert=certs, verify=False).json()
    return data[site.name]["metadata"]["xrootd"].items()

def submitDebug(site, dataIn, certs):
    # SUBMIT
    urls = str(site.query_url) + "/MAIN/sitefe/json/frontend/submitdebug/NEW"
    outs = requests.post(urls, data=dataIn, cert=certs, verify=False).json()
    return outs

def getDebugResponse(site, ID, certs):
    # GET
    urlg = str(site.query_url) + f"/MAIN/sitefe/json/frontend/getdebug/{ID}"
    outg = requests.get(urlg, cert=certs, verify=False).json()
    return outg

def deleteDebug(site, ID, certs):
    # DELETE
    urld = str(site.query_url) + f"/MAIN/sitefe/json/frontend/deletedebug/{ID}"
    outd = requests.delete(urld, cert=certs, verify=False).json()
    print(outd)

def ping(src_site, dst_site, certs):
    data_in = {'type': 'rapidping', 
               'sitename': src_site.name, 
               'hostname': src_site.one_host,  
               'ip': dst_site.one_ip, 
               'interface': dst_site.one_interface, 
               'time': '5', 
               "packetsize": "32"}
    outs = submitDebug(src_site, dataIn=data_in, certs=certs)
    sleep(10)
    outg = getDebugResponse(src_site, outs["ID"], certs)
    deleteDebug(src_site, outs["ID"], certs)
    return outg

def iperf(src_site, dst_site, time, certs):
    # src runs server, dst runs client
    data_iperf_server = {
        'type': 'iperfserver', 
        'sitename': src_site.name,
        'hostname': src_site.one_host, 
        'port': '31601',
        'ip': src_site.one_ip, 
        'time': time, 
        'onetime': 'True'
    }
    data_iperf_client = {
        "type": "iperf",
        "sitename": dst_site.name,
        "hostname": dst_site.one_host,
        "port": '31601',
        "ip": dst_site.one_ip,
        "time": time,
        "interface": dst_site.one_interface
    }
    out_server = submitDebug(src_site, dataIn=data_iperf_server, certs=certs)
    out_client = submitDebug(dst_site, dataIn=data_iperf_client, certs=certs)
    sleep(time + 10)
    outg = getDebugResponse(dst_site, out_client["ID"], certs)
    deleteDebug(src_site, out_server["ID"], certs)
    deleteDebug(dst_site, out_client["ID"], certs)
    return outg