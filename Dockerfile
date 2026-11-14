FROM golang:1.25.5-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/controller ./cmd/controller \
    && CGO_ENABLED=0 go build -o /out/worker ./cmd/worker

FROM alpine:3.22 AS controller
RUN apk add --no-cache ca-certificates
COPY --from=build /out/controller /controller
USER nobody
ENTRYPOINT ["/controller"]

FROM alpine:3.22 AS worker
RUN apk add --no-cache ca-certificates
COPY --from=build /out/worker /worker
USER nobody
ENTRYPOINT ["/worker"]
