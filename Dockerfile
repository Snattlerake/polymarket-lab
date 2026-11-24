FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
RUN useradd --system --uid 10001 --create-home appuser \
    && mkdir -p /app/data \
    && chown -R appuser:appuser /app
COPY --from=build /build/target/polymarket-lab.jar /app/polymarket-lab.jar
COPY index.html /app/index.html
RUN chown appuser:appuser /app/index.html /app/polymarket-lab.jar
USER appuser
EXPOSE 8080
ENV PORT=8080 DATA_DIR=/app/data HISTORY_DAYS=30 MARKET_LIMIT=250 WORKERS=4
VOLUME ["/app/data"]
ENTRYPOINT ["java","-XX:MaxRAMPercentage=70.0","-jar","/app/polymarket-lab.jar"]
