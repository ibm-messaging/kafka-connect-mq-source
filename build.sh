#!/bin/bash
MAVEN_VERSION="3.5.4"
curl -v "http://www-us.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz" -o maven.tar.gz
tar xzf maven.tar.gz
export MVNROOT=$PWD
export PATH=$PWD/apache-maven-${MAVEN_VERSION}/bin:$PATH

# Maven install
mvn clean
mvn package