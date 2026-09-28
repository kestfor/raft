FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /raftnode ./cmd/raftnode

FROM scratch
COPY --from=build /raftnode /raftnode
COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.38 /ko-app/grpc-health-probe /grpc_health_probe
USER 65532:65532
EXPOSE 9000
ENTRYPOINT ["/raftnode"]
