from sense.client.address_api import AddressApi
import json
import logging
import argparse

logging.basicConfig(level=logging.DEBUG)

def free_allocation(sitename, alloc_name):
    try:
        logging.debug(f"Freeing IPv6 allocation {alloc_name}")
        addressApi = AddressApi()
        pool_name = 'RUCIO_Site_BGP_Subnet_Pool-' + sitename
        addressApi.free_address(pool_name, name=alloc_name)
        logging.debug(f"Allocation {alloc_name} freed for {sitename}")
    except Exception as e:
        logging.error(f"free_allocation: {str(e)}")
        raise ValueError(f"Freeing allocation failed for {sitename} and {alloc_name}")

if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--sitename", help="Site name")
    argparser.add_argument("--alloc_name", help="Allocation name")

    args = argparser.parse_args()
    
    free_allocation(args.sitename, args.alloc_name)