FROM maven:3.9-ibm-semeru-17-noble AS build

WORKDIR /app
COPY pom.xml /app/
COPY producer-service/pom.xml /app/producer-service/
COPY producer-service/src /app/producer-service/src/

WORKDIR /app/producer-service
RUN mvn clean package -DskipTests

FROM eclipse-temurin:17-jdk-alpine

WORKDIR /app
COPY --from=build /app/producer-service/target/*.jar app.jar

RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

ENTRYPOINT ["java", "-jar", "app.jar"]