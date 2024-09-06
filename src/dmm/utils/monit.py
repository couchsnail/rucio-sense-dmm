import json
import requests
import re
import statistics
from datetime import datetime, timedelta

from dmm.utils.config import config_get

# Helper functions
def prom_submit_query(query_dict) -> dict:
    prometheus_user = config_get("prometheus", "user")
    prometheus_pass = config_get("prometheus", "password")
    prometheus_host = config_get("prometheus", "host")

    endpoint = "api/v1/query"
    query_addr = f"{prometheus_host}/{endpoint}"
    return requests.get(query_addr, params=query_dict, auth=(prometheus_user, prometheus_pass)).json()

def prom_get_val_from_response(response):
    """Extract desired value from typical location in Prometheus response"""
    print(response)
    return response["data"]["result"][0]["value"][1]
    
def prom_get_interface(ipv6) -> str:
    response = prom_submit_query({"query": "node_network_address_info"})
    if response["status"] == "success":
        for metric in response["data"]["result"]:
            if re.match(rf'{ipv6[:-3]}[0-9a-fA-F]{{1,4}}',metric["metric"]["address"]) != None:
            #if metric["metric"]["address"] == ipv6:
                return (metric["metric"]["device"], metric["metric"]["instance"], metric["metric"]["job"], metric["metric"]["sitename"])

def prom_get_all_interface(ipv6) -> str:
    """
    Gets all interfaces with ipv6 addresses matching the given ipv6
    """
    #Change ipv6_pattern if needed
    ipv6_pattern = f"{ipv6[:-3]}[0-9a-fA-F]{{1,4}}"
    query = f"node_network_address_info{{address=~'{ipv6_pattern}'}}"
    response = prom_submit_query({"query": query})

    interfaces = []
    
    #If response is a successful transfer and matches the given ipv6,
    #adds its interface data to the list
    if response["status"] == "success":        
        for metric in response["data"]["result"]:
            if re.match(ipv6_pattern, metric["metric"]["address"]):
                interfaces.append(
                    (
                        metric["metric"]["device"], 
                        metric["metric"]["instance"], 
                        metric["metric"]["job"], 
                        metric["metric"]["sitename"]
                    )
                )
    return interfaces

def prom_get_total_bytes_at_t(time, ipv6) -> float:
    """
    Returns the total number of bytes transmitted from a given Rucio RSE via a given
    ipv6 address
    """
    device, instance, job, sitename = prom_get_interface(ipv6)
    query_params = f"device=\"{device}\",instance=\"{instance}\",job=\"{job}\",sitename=\"{sitename}\""
    print(query_params)
    metric = f"node_network_transmit_bytes_total{{{query_params}}}"
    # Get bytes transferred at the start time
    response = prom_submit_query({"query": metric, "time": time})
    print(response)
    if response is not None and response["status"] == "success":
        bytes_at_t = prom_get_val_from_response(response)
    else:
        raise Exception(f"query {metric} failed")
    return float(bytes_at_t)

def prom_get_all_bytes_at_t(time, ipv6) -> float:
    """
    Returns the total number of bytes transmitted from a given Rucio RSE via a given
    ipv6 address
    """
    transfers = prom_get_all_interface(ipv6) 
    total_bytes = 0
    for transfer in transfers:
        device, instance, job, sitename = transfer[0], transfer[1], transfer[2], transfer[3]
        query_params = f"device=\"{device}\",instance=\"{instance}\",job=\"{job}\",sitename=\"{sitename}\""
        metric = f"node_network_transmit_bytes_total{{{query_params}}}"
        # Get bytes transferred at the start time
        response = prom_submit_query({"query": metric, "time": time})
        if response is not None and response["status"] == "success":
            bytes_at_t = prom_get_val_from_response(response)
        else:
            raise Exception(f"query {metric} failed")
        #Add the bytes at given time to running total
        total_bytes += float(bytes_at_t)
    return total_bytes

#t_avg_over should be time when transfer ended for given rule_id
#time stamp rule ending - time stamp provision kicking in
#The IP address here is src_ipv6 + dst_ipv6 (only src_ipv6 for now)
def prom_get_throughput_at_t(time, ipv6, t_avg_over=None) -> float:
    bytes_transmitted = sum([i * prom_get_all_bytes_at_t(time + i * 0.5 * t_avg_over, ipv6) for i in [-1,1]])
    #bytes_transmitted = sum([i * prom_get_total_bytes_at_t(time + i * 0.5 * t_avg_over, ipv6) for i in [-1,1]])
    # TODO account for bin edges
    return bytes_transmitted / (t_avg_over)

def fts_get_val_from_response(response):
    """Extract desired value from typical location in Prometheus response"""
    return response["hits"]["hits"][0]["_source"]["data"]

def fts_submit_job_query(rule_id):    
    fts_host = config_get("fts", "monit_host")
    fts_token = config_get("fts", "monit_auth_token")
    headers = {"Authorization": f"Bearer {fts_token}", "Content-Type": "application/json"}
    endpoint = "api/datasources/proxy/9233/monit_prod_fts_enr_complete*/_search"
    query_addr = f"{fts_host}/{endpoint}"
    data = {
        "size": 100,
        "query":{
            "bool":{
                "filter":[{
                    "query_string": {
                        "analyze_wildcard": "true",
                        "query": f"data.file_metadata.rule_id:{rule_id}"
                    }
                }]
            }
        },
        #"_source": ["data.tr_timestamp_start", "data.tr_timestamp_complete"]
    }
    data_string = json.dumps(data)
    response = requests.get(query_addr, data=data_string, headers=headers).json()
    #return response
    timestamps = [hit["_source"]["data"] for hit in response["hits"]["hits"]]
    return timestamps

#Returns timestamps and file size from FTS
def fts_get_timestamps(response):
    timestamps = [{'tr_timestamp_start': r['tr_timestamp_start'], 
                   'tr_timestamp_complete': r['tr_timestamp_complete'],
                   'file_size': r['file_size']} 
                   for r in response]
    
    return timestamps
    
'''
transfer state (might be redundant with circuit status), transfer start time,
                    transfer end time, transfer completion time, transfer operation time (how long it took),
                    transfer file size, volume of data transferred, number of retries, errors (if any)
'''
#Make sure to be able to call this from the rule_id on frontend (along with get_throughput_at_t)

#Objective: Find out if transfer failed/succeeded based on information in the query
                #Show the error
                #Correlate the metrics from FTS/Prometheus (do this after)

    #Throughput calculation: All FTS records that start after allocation timestamp
    #How much data is transferred at a given time - bytes/second
    #For a given rule, see the throughput (best effort, worst, etc.)
    #Need to dissect before and after - if transferring a petabyte and allocation takes 10 min
    #-See short time where throughput is bad, time when is large

    #find all transfers when throughput is low, separate from when all transfers are large
    #Middle timestamp are the ones you care about (how long it took to be transferred)

def calculate_throughput_sliding_window_fixed(transfers, start_time, end_time, window_size=None, step_size=None):
    # Converts timestamps from milliseconds into datetime format
    start_time = datetime.fromtimestamp(start_time / 1000.0)
    end_time = datetime.fromtimestamp(end_time / 1000.0)
    
    # Calculate the total seconds between start_time and end_time
    total_seconds = (end_time - start_time).total_seconds()
    #Can adjust this based on how big we want the windows to actually be
    if window_size == None or step_size==None:
        window_size = total_seconds / 20
        step_size = window_size / 2
        num_windows = int((total_seconds - window_size) // step_size) + 1
    
    throughputs = []

    #Iterates through each window
    for i in range(num_windows):
        #Calculates the window start as time added to start_time based on step size
        window_start = start_time + timedelta(seconds=i * step_size)
        #Calculates window end as time added to window start time based on window size
        window_end = window_start + timedelta(seconds=window_size)
        
        #Holds the throughput that happened within a given window
        window_throughput = 0

        for transfer in transfers:
            transfer_start, transfer_end, file_size = transfer.values()            
            # Converts timestamps from milliseconds into datetime format, may need to change timezone
            transfer_start = datetime.fromtimestamp(transfer_start / 1000.0, tz=timezone.utc)
            transfer_end = datetime.fromtimestamp(transfer_end / 1000.0, tz=timezone.utc)
            
            #If the transfer doesn't overlap with the given window, move on
            if transfer_end <= window_start or transfer_start >= window_end:
                continue
            
            #Otherwise calculate the overlap of the transfer with the given window
            overlap_start = max(window_start, transfer_start)
            overlap_end = min(window_end, transfer_end)
            overlap_seconds = (overlap_end - overlap_start).total_seconds()
            
            # Calculate the fraction of the transfer that falls within this window
            total_transfer_seconds = (transfer_end - transfer_start).total_seconds()
            transfer_fraction = (overlap_seconds / total_transfer_seconds) * file_size
            
            # Add the fraction of the file size to the window_throughput
            window_throughput += transfer_fraction
        
        #Right now throughput is in bytes per second, can change this if necessary
        throughputs.append(window_throughput / window_size)
        
    # Calculate average and standard deviation, can add additional metrics if needed
    average_throughput = sum(throughputs)/len(throughputs)
    std_deviation = statistics.pstdev(throughputs)
    
    return throughputs, average_throughput, std_deviation

if __name__ == "__main__":
   result_query = fts_submit_job_query("test2")
   print(result_query)
#    timestamps = fts_get_timestamps(result_query)
#    start_sorted_timestamps = sorted(timestamps, key=lambda x: x['tr_timestamp_start'])
#    comp_sorted_timestamps = sorted(timestamps, key=lambda x: x['tr_timestamp_complete'])
   
#    min_complete_timestamp = min(start_sorted_timestamps)
#    max_complete_timestamp = max(comp_sorted_timestamps)
   
#    now = datetime.now()
   
#    # Find how many days ago last Thursday was
#    days_since_thursday = (now.weekday() - 3) % 7  # 3 corresponds to Thursday
   
#    #Calculate the datetime for last Thursday at 10:30 AM
#    last_thursday = now - timedelta(days=days_since_thursday)
#    last_thursday_at_1030 = last_thursday.replace(hour=10, minute=30, second=0, microsecond=0)

#    # Convert to timestamp (milliseconds since epoch)
#    timestamp = int(last_thursday_at_1030.timestamp() * 1000)

#    start_bytes = prom_get_all_bytes_at_t(timestamp,"2001:48d0:3001:112::/64")
#    print(start_bytes)
#    end_bytes = prom_get_all_bytes_at_t(max_complete_timestamp,"2001:48d0:3001:112::/64")
#    print(end_bytes)

       