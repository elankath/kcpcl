# copyshoot
CLI tool that copies a Gardener Shoot data and control plane objects into a target k8s cluster


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
