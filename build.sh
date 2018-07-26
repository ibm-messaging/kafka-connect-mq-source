#!/bin/bash
curl http://www-us.apache.org/dist/maven/maven-3/3.5.3/binaries/apache-maven-3.5.3-bin.tar.gz -o maven.tar.gz
tar xzf maven.tar.gz
export MVNROOT=$PWD
export PATH=$PWD/apache-maven-3.5.3/bin:$PATH

# Maven install
mvn clean
mvn package