package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"log"
)

func main() {
	sdkConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("not found account key: %v", err)
	}
	s3Client := s3.NewFromConfig(sdkConfig)

	buckets, err := s3Client.ListBuckets(context.TODO(), nil)
	if err != nil {
		log.Fatalf("get bucket list err: %v", err)
	}

	for _, bucket := range buckets.Buckets {
		log.Printf("\t%v\n", *bucket.Name)
	}
}
