from sense.client.address_api import AddressApi
import time
import logging

logging.basicConfig(level=logging.DEBUG)

def get_free_ipv6(sitename, alloc_name):
    addressApi = AddressApi()
    pool_name = 'RUCIO_Site_BGP_Subnet_Pool-' + sitename
    alloc_type = 'IPv6'
    try:
        response = addressApi.allocate_address(pool_name, alloc_type, alloc_name, netmask='/64', batch='subnet')
        print(response)
        time.sleep(10)
        addressApi.free_address(pool_name, name=alloc_name)
    except ValueError as ex:
        addressApi.free_address(pool_name, name=alloc_name)
        raise ValueError(ex)

a = get_free_ipv6("T2_US_SDSC", "RUCIO_SENSE")
# a = get_free_ipv6("T2_US_Caltech_Test", "RULEID1_T2_US_Caltech_Test")
