#!/bin/bash

apt-get update && apt-get install -y curl
curl -sL https://deb.nodesource.com/setup_8.x | bash -
apt-get install -y nodejs
apt-get install -y gcc g++ make
/usr/bin/npm install -g node-gyp

cp /app/krb5.conf /etc/krb5.conf
kinit -kt /app/sergio.keytab sergio@MIPODO.COM

tail -f /dev/null