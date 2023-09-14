package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
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

type Chain struct {
	request  *http.Request
	response *http.Response
	data     []byte
	err      error
}

func StartRequest(ctx context.Context) *Chain {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://viacep.com.br/ws/14010090/json/", nil)
	return &Chain{
		request: req,
		err:     err,
	}
}

func (c *Chain) GetResponse(ctx context.Context) *Chain {
	if c.err != nil {
		return c
	}
	httpClient := xray.Client(http.DefaultClient)
	c.response, c.err = httpClient.Do(c.request)
	return c
}

func (c *Chain) ProcessResponse(ctx context.Context) *Chain {
	if c.err != nil {
		return c
	}
	defer c.response.Body.Close()
	c.data, c.err = io.ReadAll(c.response.Body)
	return c
}

func (c *Chain) CommitToS3(ctx context.Context) *Chain {
	if c.err != nil {
		return c
	}
	SendToS3(ctx, string(c.data))
	return c
}

func (c *Chain) EndRequest() error {
	if c.err != nil {
		log.Println("Error")
		return c.err
	}
	log.Println("Success")
	return nil
}

func api(ctx context.Context) {
	err := StartRequest(ctx).
		GetResponse(ctx).
		ProcessResponse(ctx).
		CommitToS3(ctx).
		EndRequest()
	if err != nil {
		panic(err)
	}
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
