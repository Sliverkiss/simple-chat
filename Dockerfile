FROM golang:1.23-alpine AS build
WORKDIR /src
COPY app/go.mod app/go.sum ./
RUN go mod download
COPY app/ .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /simple-chat .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /simple-chat /simple-chat
EXPOSE 8080
ENTRYPOINT ["/simple-chat"]
