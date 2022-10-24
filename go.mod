module github.com/bcongdon/corral

go 1.16

require (
	github.com/aws/aws-lambda-go v1.25.0
	github.com/aws/aws-sdk-go v1.44.121
	github.com/dustin/go-humanize v1.0.0
	github.com/ease-lab/vhive/utils/tracing/go v0.0.0-20210727103631-f5f1ba9920c2
	github.com/hashicorp/golang-lru v0.5.4
	github.com/mattetti/filebuffer v1.0.1
	github.com/mattn/go-runewidth v0.0.12 // indirect
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.8.1
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.20.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	google.golang.org/grpc v1.39.0
	google.golang.org/protobuf v1.26.0
	gopkg.in/cheggaaa/pb.v1 v1.0.28
)
