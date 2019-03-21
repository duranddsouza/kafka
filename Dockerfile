FROM openjdk:8-jre
MAINTAINER Durand Dsouza

ENTRYPOINT ["/usr/bin/java", "-jar", "/tmp/demo-1.0-SNAPSHOT-jar-with-dependencies.jar"]

ADD target/demo-1.0-SNAPSHOT-jar-with-dependencies.jar /tmp/demo-1.0-SNAPSHOT-jar-with-dependencies.jar