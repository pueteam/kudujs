# Dockerfile

# Specify a Cloudera Data Science Workbench base image
FROM docker.repository.cloudera.com/cdsw/engine:7

RUN apt-get update && apt-get -y install curl

RUN curl -sL https://deb.nodesource.com/setup_8.x | bash -

RUN apt-get install -y nodejs

RUN apt-get install -y gcc g++ make

RUN /usr/bin/npm install -g node-gyp

ADD src /home/cdsw/src

# Run the command on container startup
CMD ["tail", "-f", "/dev/null"]