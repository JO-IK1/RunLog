FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/runlog ./cmd/runlog

FROM alpine:3.23
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 runlog
COPY --from=build /out/runlog /usr/local/bin/runlog
USER runlog
EXPOSE 8080
ENTRYPOINT ["runlog"]
CMD ["api"]
