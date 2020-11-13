FROM jre-11.0.9_11-alpine
ARG JAR_FILE=bigdatatest.jar
ADD ${JAR_FILE} /opt/app.jar
ENTRYPOINT ["java","-jar","/opt/app.jar"]