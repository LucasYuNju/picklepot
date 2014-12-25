#!/bin/bash

mvn package -DskipTests

mvn install:install-file -Dfile=target/picklepot-1.0-SNAPSHOT.jar -DpomFile=pom.xml
