module github.com/snodra/snodra-messaging-go/examples/store-interop

go 1.26.0

// The example is part of this repository and builds against the add-on modules next to it: their
// tags lag the code a level is developed on. A consumer of the released modules writes the
// required versions instead and needs no replace.
require (
	github.com/snodra/snodra-messaging-go v0.5.0
	github.com/snodra/snodra-messaging-go/kafka v0.0.0
	github.com/snodra/snodra-messaging-go/mongo v0.0.0
	github.com/snodra/snodra-messaging-go/rabbitmq v0.0.0
	github.com/snodra/snodra-messaging-go/sql v0.0.0
)

require github.com/twmb/franz-go v1.22.3 // indirect

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/go-sql-driver/mysql v1.10.2 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.13.0 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/rabbitmq/rabbitmq-stream-go-client v1.10.0 // indirect
	github.com/twmb/franz-go/pkg/kadm v1.20.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.15.1 // indirect
	github.com/xdg-go/scram v1.4.0 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	go.mongodb.org/mongo-driver/v2 v2.10.1 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260817212433-ac3dfec99bb1 // indirect
	google.golang.org/grpc v1.85.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace (
	github.com/snodra/snodra-messaging-go => ../..
	github.com/snodra/snodra-messaging-go/kafka => ../../kafka
	github.com/snodra/snodra-messaging-go/mongo => ../../mongo
	github.com/snodra/snodra-messaging-go/rabbitmq => ../../rabbitmq
	github.com/snodra/snodra-messaging-go/sql => ../../sql
)
