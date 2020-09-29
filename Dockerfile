FROM openjdk:8-alpine
RUN mkdir -p /app
WORKDIR /app
ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} /app/app.jar
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/app/app.jar"]