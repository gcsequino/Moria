FROM golang:1.26-bookworm@sha256:d9c68c2c51161e12fd77e4c6320687c9cd86e1af1e3ad6e6cd63ff970641453c AS go
WORKDIR /src
COPY benchmarks/ontology/go.mod benchmarks/ontology/go.sum benchmarks/ontology/main.go ./
RUN go mod tidy && go build -o /go-server .

# Build deploy/test/Dockerfile as moria-test:local first.
FROM moria-test:local
COPY --chown=node:node benchmarks/ontology/requirements.txt benchmarks/ontology/requirements.txt
RUN .venv/bin/python -m pip install --no-cache-dir -r benchmarks/ontology/requirements.txt
COPY --chown=node:node benchmarks/ontology/ benchmarks/ontology/
COPY --from=go /go-server /app/benchmarks/ontology/go-server
COPY --from=go /src/go.sum /app/benchmarks/ontology/go.sum
CMD ["node", "--import", "tsx", "benchmarks/ontology/run.ts"]
