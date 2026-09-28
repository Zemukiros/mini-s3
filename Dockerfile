# Build stage: Maven runs inside Docker, so you don't need Java or Maven installed.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package

# Run stage: one image for all three roles. SPRING_PROFILES_ACTIVE picks the role.
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/target/mini-s3.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
