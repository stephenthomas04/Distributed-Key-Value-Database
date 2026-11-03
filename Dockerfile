FROM golang:1.26.1 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /server ./cmd/server && CGO_ENABLED=0 go build -o /kvctl ./cmd/kvctl
FROM debian:bookworm-slim
COPY --from=build /server /kvctl /usr/local/bin/
ENTRYPOINT ["server"]
