FROM eclipse-temurin:21-jre

WORKDIR /app

COPY backend/target/*.jar app.jar

RUN useradd --system --create-home --shell /usr/sbin/nologin appuser \
    && mkdir -p /data \
    && chown -R appuser:appuser /app /data

USER appuser

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
