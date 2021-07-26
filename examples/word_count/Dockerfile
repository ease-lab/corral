FROM vhiveease/golang-builder AS builder
WORKDIR /corral
COPY . .
WORKDIR /corral/examples/word_count
RUN make

FROM vhiveease/runner
RUN apt-get update && apt-get install -y ca-certificates
COPY --from=builder /corral/examples/word_count/bin/word_count /usr/local/bin/word_count
ENTRYPOINT [ "/usr/local/bin/word_count" ]
