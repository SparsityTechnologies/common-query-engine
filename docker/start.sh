#!/usr/bin/env bash

mkdir -p $HOME/.m2
touch $HOME/.m2/settings.xml

echo "<settings><servers>" >> $HOME/.m2/settings.xml
echo "<server><id>adapt03-libs</id><username>" >> $HOME/.m2/settings.xml
echo $MVN_USER >> $HOME/.m2/settings.xml
echo "</username><password>" >> $HOME/.m2/settings.xml
echo $MVN_PWD >> $HOME/.m2/settings.xml
echo "</password></server>" >> $HOME/.m2/settings.xml


echo "<server><id>adapt03</id><username>" >> $HOME/.m2/settings.xml
echo $MVN_USER >> $HOME/.m2/settings.xml
echo "</username><password>" >> $HOME/.m2/settings.xml
echo $MVN_PWD >> $HOME/.m2/settings.xml
echo "</password></server>" >> $HOME/.m2/settings.xml


echo "<server><id>adapt03-coherentpaas-snapshots</id><username>" >> $HOME/.m2/settings.xml
echo $MVN_USER >> $HOME/.m2/settings.xml
echo "</username><password>" >> $HOME/.m2/settings.xml
echo $MVN_PWD >> $HOME/.m2/settings.xml
echo "</password></server>" >> $HOME/.m2/settings.xml
echo "</servers></settings>" >> $HOME/.m2/settings.xml

echo "<server><id>adapt03-remote-libs</id><username>" >> $HOME/.m2/settings.xml
echo $MVN_USER >> $HOME/.m2/settings.xml
echo "</username><password>" >> $HOME/.m2/settings.xml
echo $MVN_PWD >> $HOME/.m2/settings.xml
echo "</password></server>" >> $HOME/.m2/settings.xml
echo "</servers></settings>" >> $HOME/.m2/settings.xml

echo "building the project"

cd /home/root/cqe && mvn clean

cd /home/root/cqe && mvn package assembly:assembly

echo "copying the cqe into the working dir"

cd /home/root/cqe/target && ls && unzip cqe-1.0-SNAPSHOT-bin.zip

cp -R /home/root/cqe/target/cqe-1.0-SNAPSHOT/* /usr/local/cqe

chmod u+x /usr/local/cqe/bin/*

echo "derby.drda.host=$HOSTNAME" > /usr/local/cqe/derby.properties


echo "starting the cqe server"

startNetworkServer -p 1527

