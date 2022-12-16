package example05version

import (
	"bytes"
	"context"
	"testing"

	"s3-demo/core/s3action"
	"s3-demo/log"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/suite"
)

type VersionSuite struct {
	suite.Suite
	S3Action s3action.S3Base
	Region   string
}

func TestVersionSuite(t *testing.T) {
	suite.Run(t, new(VersionSuite))
}

func (s *VersionSuite) SetupSuite() {
	s.S3Action = *s3action.NewS3Client()
	s.Region = "us-west-2"
}

func (s *VersionSuite) Test01PutBucketVersion() {
	bucketName := "yuki-testbucket-version-2022-12"
	err := s.S3Action.CreateBucket(bucketName, s.Region)
	log.Infof("err: %v", err)

	putInput := &s3.PutBucketVersioningInput{
		Bucket: &bucketName,
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	}
	output, err := s.S3Action.S3Client.PutBucketVersioning(context.TODO(), putInput)
	s.NoError(err)
	log.Infof("put bucket version output: %v", output)
}

func (s *VersionSuite) Test02UploadByVersion() {
	bucketName := "yuki-testbucket-version-2022-12"
	key := "yuki-testobject-version-2022-12"
	fileName := "test.csv"

	err := s.S3Action.UploadFile(bucketName, key, fileName)
	s.NoError(err)
}

func (s *VersionSuite) Test03GetVersionObject() {
	bucketName := "yuki-testbucket-version-2022-12"
	key := "yuki-testobject-version-2022-12"
	versionId := "ekZKRq4.7oLW9J_epjVkJeutmeW5RRrx"
	input := &s3.GetObjectInput{
		Bucket:    &bucketName,
		Key:       &key,
		VersionId: &versionId,
	}
	object, err := s.S3Action.S3Client.GetObject(context.TODO(), input)
	s.NoError(err)

	buf := new(bytes.Buffer)
	buf.ReadFrom(object.Body)
	log.Infof("res: %v", buf.String())
}

func (s *VersionSuite) Test04DeleteVersionObject() {
	bucketName := "yuki-testbucket-version-2022-12"
	key := "yuki-testobject-version-2022-12"
	versionId := "ekZKRq4.7oLW9J_epjVkJeutmeW5RRrx"
	input := &s3.DeleteObjectInput{
		Bucket:    &bucketName,
		Key:       &key,
		VersionId: &versionId,
	}
	_, err := s.S3Action.S3Client.DeleteObject(context.TODO(), input)
	s.NoError(err)
}

func (s *VersionSuite) Test05GetObjectVersionList() {
	bucketName := "yuki-testbucket-version-2022-12"
	input := &s3.ListObjectVersionsInput{
		Bucket: &bucketName,
	}
	versions, err := s.S3Action.S3Client.ListObjectVersions(context.TODO(), input)
	s.NoError(err)
	log.Infof("version: %v", versions.Versions)
	for _, v := range versions.Versions {
		log.Infof("ver: %v", *v.Key)
		log.Infof("ver: %v", *v.VersionId)
	}
}
