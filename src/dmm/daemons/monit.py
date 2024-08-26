import logging
import json
from datetime import datetime, timedelta

from dmm.db.session import databased, get_engine
from dmm.db.models import *
from dmm.utils.monit import prom_get_all_bytes_at_t, fts_submit_job_query, fts_get_timestamps, calculate_throughput_sliding_window_fixed
from dmm.utils.db import get_requests, get_request_cursor, update_bytes_at_t

'''
What we need to accomplish here:
1) Get all PROVISIONED transfers that have a sense circuit created
2) Get the current bandwidth being transmitted from Prometheus
3) Compare with requested bandwidth from DMM
4) Raise alert if throughput is larger - errors are occurring
'''
#Integrate this with the frontend with 3 categories: If requested - real = 10%, say it's Excellent, within 20% say OK, if > 20%, alerg
#For testing, each file is 4GB and just check 
#how much time it takes to complete and divide it by the bytes
@databased
def online_monitoring(query_frequency=10, session=None):
    #For ease of looking in the terminal
    print("###############################################################")
    logging.debug("Getting network transfer data")
    #If there are requests that are provisioned, continue
    #Otherwise return
    reqs = get_requests(status=["PROVISIONED"],session=session)
    if reqs:  
        logging.debug(f"Calculating bandwidth")
    else:
        logging.debug("No data found")
        return
    
    for req in reqs:
        #Get the assigned bandwidth and ipv6 for the given request
        bandwidth, ipv6 = req.bandwidth, req.src_ipv6_block
        logging.debug(f"Requested Bandwidth: {bandwidth}, IPV6: {ipv6}")  
        
        #Get the actual volume of bytes being transferred at current time
        timestamp = round(datetime.timestamp(datetime.now()))
        current_volume = prom_get_all_bytes_at_t(timestamp, ipv6)
        logging.debug(f"Current volume: {current_volume}")

        #Update the given volume in the database
        update_bytes_at_t(req, current_volume, query_frequency, session=session)
        bandwidth_volumes = req.bytes_at_t
        logging.debug(f"Data: {bandwidth_volumes}")

        #Take difference in amount of bytes that was transferred over time
        #y2 - y1 / t2 - t1 for all 5 intervals and then average those out        
        change_in_throughput = change_in_prom_throughput_calc(req.bytes_at_t, query_frequency,session=session)
        throughput = prom_throughput_calc(req.bytes_at_t,query_frequency,session=session)
        logging.debug(f"Requested throughput: {bandwidth}")
        logging.debug(f"Change in throughput: {change_in_throughput}")
        logging.debug(f"Current throughput: {throughput}")

        if req.total_sec == 0:
            logging.debug(f"Alternate throughput calculation: {req.total_bytes}")
        else:
            alt_throughput = req.total_bytes / req.total_sec
            logging.debug(f"Alternate throughput calculation: {alt_throughput}")
        
#This monitors the change in throughput as time goes on
@databased
def change_in_prom_throughput_calc(bytes, interval,session=None):
    avg_throughput_bytes = 0
    avg_throughput_bytes += abs(bytes['interval_1']-bytes['interval_2'])
    avg_throughput_bytes += abs(bytes['interval_2']-bytes['interval_3'])
    avg_throughput_bytes += abs(bytes['interval_3']-bytes['interval_4'])
    avg_throughput_bytes += abs(bytes['interval_4']-bytes['interval_5'])
    avg_throughput_bytes /= (4 * interval)
    avg_throughput_GBytes = (avg_throughput_bytes / (1024 ** 3)) * 8
    return avg_throughput_GBytes

#Need a different throughput calc
@databased
def prom_throughput_calc(bytes, interval,session=None):
    bytes_dif = bytes['interval_5'] - bytes['interval_1']
    bytes_dif /= (4 * interval)
    bytes_dif = (bytes_dif/(1024**3))
    return bytes_dif
'''
What we need to accomplish here:
1) Get all PROVISIONED transfers that have a sense circuit created
2) Get the bandwidth transmitted from Prometheus from start to end
3) Compare with total file_size of data from FTS
4) Raise alert if throughput is larger - transfers failed
Ideally FTS, Prometheus, assigned bandwidth should all match
If a site was in good shape, it should also match the elevated bandwidth
For Prometheus checks, sample the total duration / 10
Record the average every hour
Prometheus: Calculate initial+last transfer, get last count of bytes, 
then divide by the total time passed
'''
#Right now what this function gets the average throughput of FINISHED transfers from FTS
#Need to clarify what to further do with this information
@databased
def offline_monitoring(session=None):
    print("---------------------------------------------")
    reqs = get_requests(status=["FINISHED"],session=session)
    if reqs == []:
        logging.debug("No transfers complete")
        return
    else:
        logging.debug("Getting finished transfers")
        for req in reqs:
            #Adjust for actual throughput calculation
            given_throughput = req.bandwidth
            logging.debug(f"Given debug: {given_throughput}")
            fts_data = fts_submit_job_query(req.rule_id)
            timestamps = fts_get_timestamps(fts_data)
            start_timestamps = [entry['tr_timestamp_start'] for entry in timestamps]
            complete_timestamps = [entry['tr_timestamp_complete'] for entry in timestamps]
            min_complete_timestamp = min(start_timestamps)
            max_complete_timestamp = max(complete_timestamps)
            #Change parameters for window_size, step_size
            throughputs, avg_throughput, std_deviation = calculate_throughput_sliding_window_fixed(timestamps, min_complete_timestamp, 
                                                                              max_complete_timestamp)
            
            logging.debug(f"Throughputs per window: {throughputs}")
            logging.debug(f"Average throughput: {avg_throughput}")
            logging.debug(f"Standard deviation of throughput: {std_deviation}")
            
            circuit_start = req.sense_provisioned_at
            transfer_end = datetime.fromtimestamp(max_complete_timestamp / 1000.0)
            time_dif = (transfer_end - circuit_start).total_seconds()
            start_bytes = prom_get_all_bytes_at_t(circuit_start, req.src_ipv6_block)
            end_bytes = prom_get_all_bytes_at_t(transfer_end, req.src_ipv6_block)
            prom_throughput = (start_bytes + end_bytes) / time_dif
            logging.debug(f"Prometheus throughput: {prom_throughput}")
            
            # #Alternative calculation
            # window = time_dif / 10
            # prom_throughput = 0
            # for i in range(10):
            #     time = circuit_start + timedelta(i * window)
            #     prom_throughput += (prom_get_all_bytes_at_t(time, req.src_ipv6_block))/time_dif
            # prom_throughput = prom_throughput / 10
            # logging.debug(f"Prometheus throughput: {prom_throughput}")

            # if actual_throughput > estimated_throughput:
            #     logging.error(f"Transfers for rule_id: {rule_id} failed")
            # elif actual_throughput < estimated_throughput:
            #     logging.error(f"Transfers for rule_id: {rule_id} had lower throughput than expected")

