#!/bin/bash

docker build . -t aaarora/dmm-dev

docker run -it --rm \
--network host \
-v $HOME/junk/dmm.cfg:/opt/dmm/dmm.cfg \
-v $HOME/junk/rucio.cfg:/opt/rucio/etc/rucio.cfg \
-v $HOME/private/certs/rucio-sense/:/opt/certs \
-v $HOME/.sense-o-auth.yaml:/root/.sense-o-auth.yaml \
-v /etc/grid-security/certificates/:/etc/grid-security/certificates \
--name dmm-dev \
aaarora/dmm-dev