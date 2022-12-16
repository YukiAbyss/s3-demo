package example02bucket

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

type BucketSuite struct {
	suite.Suite
	S3Action   *s3action.S3Base
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
	listBuckets, err := s.S3Action.GetBucketList()
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

func (s *BucketSuite) Test05GetBucketAcl() {
	bucketName := "yuki-testobject-2022-12"
	gbao, err := s.S3Action.S3Client.GetBucketAcl(context.TODO(), &s3.GetBucketAclInput{Bucket: &bucketName})
	s.NoError(err)
	log.Infof("res: %v", *gbao.Owner.DisplayName)
	log.Infof("res: %v", *gbao.Owner.ID)

	for _, g := range gbao.Grants {
		if g.Grantee.DisplayName == nil {
			log.Infof("Grantee: EVERYONE")
		} else {
			log.Infof("Grantee: %v", *g.Grantee.DisplayName)
		}

		log.Infof("Type: %v", string(g.Grantee.Type))
		log.Infof("Permission: %v", string(g.Permission))
		log.Infof("")
	}
}

func (s *BucketSuite) Test06PutBucketAcl() {
	bucketName := "yuki-testobject-2022-12"

	input := &s3.PutBucketAclInput{
		Bucket: &bucketName,
		ACL:    types.BucketCannedACLPublicReadWrite,
	}
	pbao, err := s.S3Action.S3Client.PutBucketAcl(context.TODO(), input)
	s.NoError(err)
	log.Info(pbao)
}

func (s *BucketSuite) Test07PutObjectAcl() {
	bucketName := "yuki-testobject-2022-12"
	key := "yuki-test-object-csv"

	input := &s3.PutObjectAclInput{
		Bucket: &bucketName,
		Key:    &key,
		ACL:    types.ObjectCannedACLPublicReadWrite,
	}

	poao, err := s.S3Action.S3Client.PutObjectAcl(context.TODO(), input)
	s.NoError(err)
	log.Info(poao)
}

func (s *BucketSuite) Test08PutBucketVersion() {
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

func (s *BucketSuite) Test09UploadByVersion() {
	bucketName := "yuki-testbucket-version-2022-12"
	key := "yuki-testobject-version-2022-12"
	fileName := "test.csv"

	err := s.S3Action.UploadFile(bucketName, key, fileName)
	s.NoError(err)
}

func (s *BucketSuite) Test10GetVersionObject() {
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

func (s *BucketSuite) Test11DeleteVersionObject() {
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

func (s *BucketSuite) Test12GetObjectVersionList() {
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
