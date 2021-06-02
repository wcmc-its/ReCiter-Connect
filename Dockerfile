FROM adoptopenjdk/openjdk11:alpine-jre
RUN mkdir -p /app
WORKDIR /app
ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} /app/app.jar
COPY ./enableLegacyTLS.security /app/enableLegacyTLS.security
CMD java -Djava.security.egd=file:/dev/./urandom -Djava.security.properties=/app/enableLegacyTLS.security -jar /app/app.jar