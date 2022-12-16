package example03object

import (
	"s3-demo/core/s3action"
	"s3-demo/log"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ObjectSuite struct {
	suite.Suite
	S3Action   *s3action.S3Base
	BucketName string
	Region     string
	FileName   string
	ObjectKey  string
}

func TestObjectSuite(t *testing.T) {
	suite.Run(t, new(ObjectSuite))
}

func (s *ObjectSuite) SetupSuite() {
	s.S3Action = s3action.NewS3Client()
	s.BucketName = "yuki-testobject-2022-12"
	s.Region = "us-west-2"
	s.FileName = "test.csv"
	s.ObjectKey = "yuki-test-object-csv"
	// s.NoError(s.S3Action.CreateBucket(s.BucketName, s.Region))
}

func (s *ObjectSuite) TearDownSuite() {
	// s.NoError(s.S3Action.DeleteBucket(s.BucketName))
}

func (s *ObjectSuite) Test01Upload() {
	err := s.S3Action.UploadFile(s.BucketName, s.ObjectKey, s.FileName)
	s.NoError(err)
}

func (s *ObjectSuite) Test02GetObjectList() {
	objects, err := s.S3Action.GetObjectList(s.BucketName)
	s.NoError(err)
	for _, obj := range objects {
		log.Infof("object: %v", *obj.Key)
		log.Infof("object: %v", *obj.ETag)
	}
}

func (s *ObjectSuite) Test03GetObject() {
	s2, err := s.S3Action.GetObjectContent(s.BucketName, s.ObjectKey)
	s.NoError(err)
	log.Infof("get obj: %v", s2)
}

func (s *ObjectSuite) Test04GetObjectUrl() {
	s2, err := s.S3Action.GetObjectUrl(s.BucketName, s.ObjectKey)
	s.NoError(err)
	log.Infof("get obj: %v", s2)
}

func (s *ObjectSuite) Test04DeleteObjectListByKeys() {
	objects, err := s.S3Action.GetObjectList(s.BucketName)
	keyList := make([]string, len(objects))
	for i, obj := range objects {
		keyList[i] = *obj.Key
	}
	s.NoError(err)
	err = s.S3Action.DeleteObjectListByKeys(s.BucketName, keyList)
	s.NoError(err)
}
