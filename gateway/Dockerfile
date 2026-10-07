FROM golang:1.22-alpine AS build
WORKDIR /app
COPY go.mod main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /paycore-gateway .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /paycore-gateway /usr/local/bin/paycore-gateway
EXPOSE 8090
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/paycore-gateway"]
