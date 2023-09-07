# DMM
Data Movement Manager for the Rucio-SENSE interoperation prototype 

## Setup
### Running in Kubernetes (Recommended)
1. Create Configuration Secrets
```
kubectl create secret generic sense-config --from-file ~/.sense-o-auth.yaml 
kubectl create secret generic dmm-config --from-file=dmm.cfg
```
2. Create Deployment
```
kubectl apply -f deploy.yaml
```
### Running in Docker
```
docker run -v .sense-o-auth.yaml:/root/.sense-o-auth.yaml -v dmm.cfg:/opt/dmm/dmm.cfg -d -p 5000:5000 aaarora/dmm:latest
```
### Building from Source
1. Install dependencies
```
pip3 install sense-o-api==1.23 sqlalchemy psycopg2-binary
```
2. Set Env Variables
```
source setup.sh
```
3. Start DMM 
```
./bin/dmm --loglevel debug
```