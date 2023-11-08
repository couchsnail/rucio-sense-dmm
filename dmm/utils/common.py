from time import sleep

def get_request_id(rule_id, src_rse_name, dst_rse_name):
    return f"{rule_id}_{src_rse_name}_{dst_rse_name}"

def wait(condition, timeout):
    time = 0
    while ((not condition) and (time < timeout)):
        sleep(10)
        time += 10
    if time > timeout:
        return False
    