#!/bin/bash

#
# "nytcorpus"
#

# Delete existing core:
sudo -u solr /opt/solr/bin/solr delete -c "nytcorpus"

# Delete existing config:
sudo rm -rf /opt/solr/server/solr/configsets/nytcorpus

# Copy new config to "/opt/solr" directory:
sudo cp -R nytcorpus /opt/solr/server/solr/configsets/

# Permissions:
sudo chown -R solr:solr /opt/solr/server/solr/configsets/nytcorpus
sudo chmod ug+rx /opt/solr/server/solr/configsets/nytcorpus

sudo find /opt/solr/server/solr/configsets/nytcorpus -type d -exec chown solr:solr {} \;
sudo find /opt/solr/server/solr/configsets/nytcorpus -type d -exec chmod 0750 {} \;

sudo find /opt/solr/server/solr/configsets/nytcorpus -type f -exec chown solr:solr {} \;
sudo find /opt/solr/server/solr/configsets/nytcorpus -type f -exec chmod 0640 {} \;

# Create core:
sudo -u solr /opt/solr/bin/solr create -c "nytcorpus" -d "nytcorpus"


## 