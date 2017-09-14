FROM golang:1.8.3 as builder
WORKDIR /go/src/tiedot/
COPY ./    .
WORKDIR /go/
RUN go get -v -d tiedot
RUN CGO_ENABLED=0 GOOS=linux go install -a -installsuffix cgo -v tiedot

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /go/bin/tiedot /bin/
RUN chmod +x /bin/tiedot
EXPOSE 8080
VOLUME ["/data"]
CMD ["/bin/tiedot","-dir","/data","-port","8080","-mode","httpd"]
