FROM alpine
COPY . /app
RUN du -sm /app/* | sort -n
