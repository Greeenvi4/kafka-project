FROM maven:3.9-ibm-semeru-17-noble AS build

WORKDIR /app
COPY pom.xml /app/
COPY consumer-service/pom.xml /app/consumer-service/
COPY consumer-service/src /app/consumer-service/src/

WORKDIR /app/consumer-service
RUN mvn clean package -DskipTests

FROM eclipse-temurin:17-jdk-alpine

WORKDIR /app
COPY --from=build /app/consumer-service/target/*.jar app.jar

RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

ENTRYPOINT ["java", "-jar", "app.jar"]