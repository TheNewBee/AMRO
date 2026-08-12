FROM nginx:1.27-alpine

ENV BACKEND_BASE_URL=http://host.docker.internal:8080

COPY docker/frontend/nginx.conf.template /etc/nginx/templates/default.conf.template
COPY frontend/dist/ /tmp/frontend-dist/

RUN set -eux; \
    index_file="$(find /tmp/frontend-dist -type f -name index.html -print -quit)"; \
    test -n "$index_file"; \
    rm -rf /usr/share/nginx/html/*; \
    cp -a "$(dirname "$index_file")"/. /usr/share/nginx/html/; \
    rm -rf /tmp/frontend-dist
