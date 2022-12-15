package example02bucket

import (
	"testing"

	"s3-demo/core/s3action"
	"s3-demo/log"

	"github.com/stretchr/testify/suite"
)

type BucketSuite struct {
	suite.Suite
	S3Action   *s3action.BucketBasics
	BucketName string
	Region     string
}

func TestBucketSuite(t *testing.T) {
	suite.Run(t, new(BucketSuite))
}

func (s *BucketSuite) SetupSuite() {
	s.S3Action = s3action.NewS3Client()
	s.BucketName = "yuki-testbucket-2022"
	s.Region = "us-west-2"
}

func (s *BucketSuite) Test01CreateBucket() {
	err := s.S3Action.CreateBucket(s.BucketName, s.Region)
	s.NoError(err)
}

func (s *BucketSuite) Test02BucketExists() {
	exists, err := s.S3Action.BucketExists(s.BucketName)
	s.NoError(err)
	log.Infof("bucket is exists: %v", exists)
}

func (s *BucketSuite) Test03GetBucketList() {
	listBuckets, err := s.S3Action.ListBuckets()
	s.NoError(err)
	for _, bucket := range listBuckets {
		log.Infof("bucket name: %v", *bucket.Name)
		log.Infof("bucket name: %v", bucket.CreationDate)
	}
}

func (s *BucketSuite) Test04DeleteBucket() {
	err := s.S3Action.DeleteBucket(s.BucketName)
	s.NoError(err)
	exists, err := s.S3Action.BucketExists(s.BucketName)
	s.NoError(err)
	log.Infof("delete bucket %v, exists: %v", exists)
}
