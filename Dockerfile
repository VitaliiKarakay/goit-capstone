FROM node:22-alpine AS web
WORKDIR /src/web
COPY web/package.json ./
RUN npm install
COPY web/ ./
RUN npm run build

FROM golang:1.25.5-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=web /src/web/dist ./web/dist
RUN CGO_ENABLED=0 go build -tags embedui -o /out/controller ./cmd/controller \
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
