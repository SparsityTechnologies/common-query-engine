FROM ubuntu:14.04
MAINTAINER Raquel Pau <rpau@ac.upc.edu>

RUN apt-get update && apt-get install -y python-software-properties software-properties-common

RUN add-apt-repository ppa:webupd8team/java

RUN echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 boolean true" | debconf-set-selections

RUN apt-get update && \
    apt-get install -y oracle-java8-installer maven unzip


# Define commonly used JAVA_HOME variable
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

EXPOSE 1527 

RUN wget --user sparsity --password sparsity123 http://adapt03.ls.fi.upm.es:8081/artifactory/ext-coherentpaas-snapshots-local/eu/coherentpaas/cqe-server-binaries/1.2-SNAPSHOT/cqe-server-binaries-1.2-20160713.081944-20-bin.zip

RUN unzip cqe-server-binaries-1.2-20160713.081944-20-bin.zip 

RUN mv cqe-server-binaries-1.2-SNAPSHOT /usr/local

RUN mv /usr/local/cqe-server-binaries-1.2-SNAPSHOT /usr/local/cqe

ENV CQE_HOME /usr/local/cqe

RUN ls -lrt /usr/local/cqe

ENV DERBY_SYSTEM /usr/local/cqe

ENV PATH $PATH:$CQE_HOME/bin

WORKDIR /usr/local/cqe

CMD ["cpaasCQEServerr"]