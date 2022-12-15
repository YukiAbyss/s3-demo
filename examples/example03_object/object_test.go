package example03object

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"s3-demo/core/s3action"
	"s3-demo/log"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ObjectSuite struct {
	suite.Suite
	S3Action   *s3action.BucketBasics
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
}

func (s *ObjectSuite) Test01CreateBucket() {
	err := s.S3Action.CreateBucket(s.BucketName, s.Region)
	s.NoError(err)
}

func (s *ObjectSuite) Test02Upload() {
	err := s.S3Action.UploadFile(s.BucketName, s.ObjectKey, s.FileName)
	s.NoError(err)
}

func (s *ObjectSuite) Test03GetObjectList() {
	objects, err := s.S3Action.ListObjects(s.BucketName)
	s.NoError(err)
	log.Infof("get object list: %v", objects)
	for _, obj := range objects {
		log.Infof("object: %+v", obj.Key)
	}
}

func (s *ObjectSuite) TestGetObject() {
	input := &s3.GetObjectInput{
		Bucket: &s.BucketName,
		Key:    &s.ObjectKey,
	}
	obj, err := s.S3Action.S3Client.GetObject(context.TODO(), input)
	s.NoError(err)
	log.Infof("get obj: %+v", obj)
	log.Infof("mate: %v", obj.Metadata)

	buf := new(bytes.Buffer)
	buf.ReadFrom(obj.Body)
	myFileContentAsString := buf.String()
	log.Infof("obj data: %v", myFileContentAsString)
}

func (s *ObjectSuite) TestCopyObjectInBucket() {
}

func (s *ObjectSuite) TestDeleteObjectList() {
	objects, err := s.S3Action.ListObjects(s.BucketName)
	keyList := make([]string, len(objects))
	for i, obj := range objects {
		keyList[i] = *obj.Key
	}
	s.NoError(err)
	err = s.S3Action.DeleteObjects(s.BucketName, keyList)
	s.NoError(err)
}
