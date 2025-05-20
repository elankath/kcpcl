# copyshoot
CLI tool that supports download and upload operations which download objects from a Gardener shoot cluster into an object directory and upload objects from that directory into a target k8s cluster. This is primarily meant to load a "test" cluster with "real" world cluster data with minimal latency.


## Usage

### End user

1. Install: `go install github.com/elankath/copyshoot`
2. See help: `copyshoot -h`


### Developer

Use the `Makefile` targets

1. See Available Make Targets:  `make help`
1. Generate viewer kubeconfigs for a cluster: `make genkubeconfig`
   1. This creates viewer kubeconfigs for a cluster and downloads them into the `gen` folder.
1. Build Binary: `make build`
1. Execute Download: `./bin/copyshoot -k gen/<cluster-name>.yaml -d /tmp/<cluster-name>`
   1. Example: `./bin/copyshoot download -k gen/garden-i034796--aw-external.yaml -d /tmp/aw`
1. Execute Upload: `./bin/copyshoot -k gen/<cluster-name>.yaml -d /tmp/<cluster-name>`
   1. Example: `./bin/copyshoot upload -k /tmp/kvcl.yaml -d /tmp/aw` #Using virtual cluster from https://github.com/unmarshall/kvcl
