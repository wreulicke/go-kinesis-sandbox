package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/kinesis"
	consumer "github.com/wreulicke/kinesis-consumer"
	ddb "github.com/wreulicke/kinesis-consumer/checkpoint/ddb"
)

type Event struct {
	Name string `json:"string"`
}

func main() {
	config := aws.NewConfig().
		WithEndpoint("localhost:4568").
		WithCredentials(credentials.NewStaticCredentials("dummy", "dummy", "dummy")).
		WithMaxRetries(10).
		WithDisableSSL(true).
		WithRegion("us-west-2")
	ddbconfig := aws.NewConfig().
		WithEndpoint("localhost:4569").
		WithCredentials(credentials.NewStaticCredentials("dummy", "dummy", "dummy")).
		WithMaxRetries(10).
		WithDisableSSL(true).
		WithRegion("us-west-2")

	checkpoint, err := ddb.New("test-stream-app", "test-stream-app", ddb.WithClient(dynamodb.New(session.New(ddbconfig))))

	if err != nil {
		fmt.Println("error occured during creating checkpointer", err)
		return
	}

	k := consumer.NewKinesisClientWithConfig(session.New(config))

	c, err := consumer.New("test-stream", consumer.WithClient(k), consumer.WithCheckpoint(checkpoint))
	if err != nil {
		fmt.Println(err)
		return
	}

	err = c.Scan(context.TODO(), func(rs *kinesis.Record) bool {
		fmt.Printf("time: %s, partition-key: %s seq-num:%s\n", rs.ApproximateArrivalTimestamp, *rs.PartitionKey, *rs.SequenceNumber)
		e := &Event{}
		if err := json.Unmarshal(rs.Data, e); err != nil {
			fmt.Println(err)
			return false
		}
		fmt.Println("sss", e)
		return true
	})

	if err != nil {
		fmt.Println(err)
		return
	}
}
