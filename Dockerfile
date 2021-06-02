FROM openjdk:8-alpine
RUN mkdir -p /app
WORKDIR /app
ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} /app/app.jar
#COPY ./enableLegacyTLS.security /app/enableLegacyTLS.security -Djava.security.properties=/app/enableLegacyTLS.security
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/app/app.jar"]