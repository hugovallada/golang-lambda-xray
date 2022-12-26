package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-xray-sdk-go/instrumentation/awsv2"
	"github.com/aws/aws-xray-sdk-go/xray"
)

func main() {
	lambda.Start(HandleRequest)
}

type MyEvent struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func HandleRequest(ctx context.Context, event MyEvent) {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://viacep.com.br/ws/14010090/json/", nil)
	if err != nil {
		panic(err)
	}
	httpClient := xray.Client(http.DefaultClient)
	res, err := httpClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}
	SendToS3(ctx, string(data))
}

func SendToS3(ctx context.Context, payload string) {
	bucket := createBucket(ctx)

	putObject := createPutObject(ctx, payload)
	_, err := bucket.PutObject(ctx, &putObject)

	if err != nil {
		panic(err)
	}
}

func createPutObject(ctx context.Context, payload string) s3.PutObjectInput {
	// Poderiamos usar esse subsegment para dar detalhes da duração do processamento
	_, root := xray.BeginSubsegment(ctx, "processamento-dados")
	defer root.Close(nil)
	return s3.PutObjectInput{
		Bucket: aws.String("testebucket-hlvls"),
		Key:    aws.String(fmt.Sprintf("%d.json", time.Now().UnixMilli())),
		Body:   bytes.NewReader([]byte(payload)),
	}
}

func createBucket(ctx context.Context) s3.Client {
	return *s3.NewFromConfig(createConfiguration(ctx))
}

func createConfiguration(ctx context.Context) aws.Config {
	cfg, err := config.LoadDefaultConfig(ctx)
	cfg.Region = "us-east-1"
	awsv2.AWSV2Instrumentor(&cfg.APIOptions) // Só isso aqui ja é suficiente para pegar a chamada do s3
	if err != nil {
		panic("Config not available")
	}
	return cfg
}
