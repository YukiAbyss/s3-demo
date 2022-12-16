package example04permission

import (
	"context"
	"testing"

	"s3-demo/core/s3action"
	"s3-demo/log"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/suite"
)

type PremissionSuite struct {
	suite.Suite
	S3Action s3action.S3Base
	Region   string
}

func TestPremissionSuite(t *testing.T) {
	suite.Run(t, new(PremissionSuite))
}

func (s *PremissionSuite) SetupSuite() {
	s.S3Action = *s3action.NewS3Client()
	s.Region = "us-west-2"
}

func (s *PremissionSuite) Test01GetBucketAcl() {
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

func (s *PremissionSuite) Test02PutBucketAcl() {
	bucketName := "yuki-testobject-2022-12"

	input := &s3.PutBucketAclInput{
		Bucket: &bucketName,
		ACL:    types.BucketCannedACLPublicReadWrite,
	}
	pbao, err := s.S3Action.S3Client.PutBucketAcl(context.TODO(), input)
	s.NoError(err)
	log.Info(pbao)
}

func (s *PremissionSuite) Test03PutObjectAcl() {
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
