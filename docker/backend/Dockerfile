FROM eclipse-temurin:21-jre

WORKDIR /app

COPY backend/target/*.jar app.jar
COPY Input.txt /seed/Input.txt
COPY docker/backend/entrypoint.sh /entrypoint.sh

RUN useradd --system --create-home --shell /usr/sbin/nologin appuser \
    && mkdir -p /data \
    && chmod +x /entrypoint.sh \
    && chown -R appuser:appuser /app /data /seed /entrypoint.sh

USER appuser

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
