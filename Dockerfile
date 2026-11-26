FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build

COPY pom.xml .
COPY src ./src

RUN mvn -q clean package

FROM eclipse-temurin:21-jre
WORKDIR /app

COPY --from=build /build/target/polymarket-lab-2.0.0-jar-with-dependencies.jar app.jar

RUN mkdir -p /app/data
EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
