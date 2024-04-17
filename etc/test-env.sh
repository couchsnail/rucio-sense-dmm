#!/bin/bash

docker build . --network host -t aaarora/dmm-dev 

docker run -it --rm \
--network host \
-v $HOME/private/dmm.cfg:/opt/dmm/dmm.cfg \
-v $HOME/private/rucio.cfg:/opt/rucio/etc/rucio.cfg \
-v $HOME/private/certs/rucio-sense/:/opt/certs \
-v $HOME/.sense-o-auth.yaml:/root/.sense-o-auth.yaml \
-v /etc/grid-security/certificates/:/etc/grid-security/certificates \
--name dmm-dev \
aaarora/dmm-dev
