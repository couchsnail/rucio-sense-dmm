<img src="assets/dmm.png" width=164/>

# DMM
Data Movement Manager (DMM) for the Rucio-SENSE interoperation prototype.

DMM is the interface between Rucio (/FTS) and SENSE, making SDN operated HEP data-flows possible

Based on the relative priorities of the datasets we construct peer-to-peer private vlans with respective dedicated bandwidths to ensure we have accountability in the use of resources.

## Setup
### Running in Kubernetes (Recommended)
1. Create Configuration Secrets (see etc/mksecrets.sh)
2. Create Deployment
```
kubectl apply -f etc/deploy.yaml
```