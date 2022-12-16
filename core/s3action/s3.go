package s3action

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type S3Base struct {
	S3Client *s3.Client
}

func NewS3Client() *S3Base {
	sdkConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("Couldn't load default configuration. Have you set up your AWS account?, err: %v", err)
	}
	s3Client := s3.NewFromConfig(sdkConfig)
	return &S3Base{S3Client: s3Client}
}

func (s *S3Base) GetBucketList() ([]types.Bucket, error) {
	result, err := s.S3Client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	var buckets []types.Bucket
	if err != nil {
		log.Printf("Couldn't list buckets for your account. Here's why: %v\n", err)
	} else {
		buckets = result.Buckets
	}
	return buckets, err
}

func (s *S3Base) BucketExists(bucketName string) (bool, error) {
	_, err := s.S3Client.HeadBucket(context.TODO(), &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	exists := true
	if err != nil {
		var apiError smithy.APIError
		if errors.As(err, &apiError) {
			switch apiError.(type) {
			case *types.NotFound:
				log.Printf("Bucket %v is available.\n", bucketName)
				exists = false
				err = nil
			default:
				log.Printf("Either you don't have access to bucket %v or another error occurred. "+
					"Here's what happened: %v\n", bucketName, err)
			}
		}
	} else {
		log.Printf("Bucket %v exists and you already own it.", bucketName)
	}

	return exists, err
}

func (s *S3Base) CreateBucket(name string, region string) error {
	_, err := s.S3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(name),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		},
	})

	if err != nil {
		log.Printf("Couldn't create bucket %v in Region %v. Here's why: %v\n",
			name, region, err)
	}
	return err
}

func (s *S3Base) CreatePublicBucket(bucketName, region string) error {
	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
		ACL:    types.BucketCannedACLPublicReadWrite,
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		},
	}
	_, err := s.S3Client.CreateBucket(context.TODO(), input)
	if err != nil {
		return err
	}
	return nil
}

func (s *S3Base) CreateBucketAndEnabledVersion(bucketName, region string) error {
	err := s.CreatePublicBucket(bucketName, region)
	if err != nil {
		return err
	}

	putInput := &s3.PutBucketVersioningInput{
		Bucket: &bucketName,
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	}
	_, err = s.S3Client.PutBucketVersioning(context.TODO(), putInput)
	if err != nil {
		return err
	}
	return nil
}

func (s *S3Base) PutPublicBucketAcl(bucketName string) error {
	putInput := &s3.PutBucketAclInput{
		Bucket: aws.String(bucketName),
		ACL:    types.BucketCannedACLPublicReadWrite,
	}
	_, err := s.S3Client.PutBucketAcl(context.TODO(), putInput)
	if err != nil {
		return err
	}
	return nil
}

func (s *S3Base) DeleteBucket(bucketName string) error {
	_, err := s.S3Client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName)})
	if err != nil {
		log.Printf("Couldn't delete bucket %v. Here's why: %v\n", bucketName, err)
	}
	return err
}

func (s *S3Base) UploadFile(bucketName string, objectKey string, fileName string) error {
	file, err := os.Open(fileName)
	if err != nil {
		log.Printf("Couldn't open file %v to upload. Here's why: %v\n", fileName, err)
	} else {
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {
				log.Fatalf("file close err: %v", err)
			}
		}(file)

		_, err := s.S3Client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   file,
		})
		if err != nil {
			log.Printf("Couldn't upload file %v to %v:%v. Here's why: %v\n",
				fileName, bucketName, objectKey, err)
		}
	}
	return err
}

func (s *S3Base) UploadLargeObject(bucketName string, objectKey string, largeObject []byte) error {
	largeBuffer := bytes.NewReader(largeObject)
	var partMiBs int64 = 10
	uploader := manager.NewUploader(s.S3Client, func(u *manager.Uploader) {
		u.PartSize = partMiBs * 1024 * 1024
	})
	_, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   largeBuffer,
	})
	if err != nil {
		log.Printf("Couldn't upload large object to %v:%v. Here's why: %v\n",
			bucketName, objectKey, err)
	}

	return err
}

func (s *S3Base) DownloadFile(bucketName string, objectKey string, fileName string) error {
	result, err := s.S3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		log.Printf("Couldn't get object %v:%v. Here's why: %v\n", bucketName, objectKey, err)
		return err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Fatalf("body close err: %v", err)
		}
	}(result.Body)
	file, err := os.Create(fileName)
	if err != nil {
		log.Printf("Couldn't create file %v. Here's why: %v\n", fileName, err)
		return err
	}

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalf("file close err: %v", err)
		}
	}(file)

	body, err := io.ReadAll(result.Body)
	if err != nil {
		log.Printf("Couldn't read object body from %v. Here's why: %v\n", objectKey, err)
	}
	_, err = file.Write(body)
	return err
}

func (s *S3Base) DownloadLargeObject(bucketName string, objectKey string) ([]byte, error) {
	var partMiBs int64 = 10
	downloader := manager.NewDownloader(s.S3Client, func(d *manager.Downloader) {
		d.PartSize = partMiBs * 1024 * 1024
	})
	buffer := manager.NewWriteAtBuffer([]byte{})
	_, err := downloader.Download(context.TODO(), buffer, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		log.Printf("Couldn't download large object from %v:%v. Here's why: %v\n",
			bucketName, objectKey, err)
	}
	return buffer.Bytes(), err
}

func (s *S3Base) GetObjectContent(bucketName, key string) (string, error) {
	output, err := s.S3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		return "", err
	}
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(output.Body)
	if err != nil {
		log.Fatalf("get object body format err: %v", err)
	}
	return buf.String(), err
}

func (s *S3Base) GetObjectUrl(bucketName, key string) (string, error) {
	presignClient := s3.NewPresignClient(s.S3Client)

	presignParams := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}

	// Apply an expiration via an option function
	presignDuration := func(po *s3.PresignOptions) {
		po.Expires = 5 * time.Minute
	}

	presignResult, err := presignClient.PresignGetObject(context.TODO(), presignParams, presignDuration)
	if err != nil {
		return "", err
	}
	return presignResult.URL, err
}

func (s *S3Base) GetObjectList(bucketName string) ([]types.Object, error) {
	result, err := s.S3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	var contents []types.Object
	if err != nil {
		log.Printf("Couldn't list objects in bucket %v. Here's why: %v\n", bucketName, err)
	} else {
		contents = result.Contents
	}
	return contents, err
}

func (s *S3Base) DeleteObject(bucketName string, object types.Object) error {
	_, err := s.S3Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: &bucketName,
		Key:    object.Key,
	})
	if err != nil {
		log.Printf("Couldn't delete objects %v from bucket %v. Here's why: %v\n", object.Key, bucketName, err)
	}
	return err
}

func (s *S3Base) DeleteObjectList(bucketName string, objectList []types.Object) error {
	var objectIds []types.ObjectIdentifier
	for _, obj := range objectList {
		objectIds = append(objectIds, types.ObjectIdentifier{Key: aws.String(*obj.Key)})
	}
	_, err := s.S3Client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &types.Delete{Objects: objectIds},
	})
	if err != nil {
		log.Printf("Couldn't delete objects from bucket %v. Here's why: %v\n", bucketName, err)
	}
	return err
}

func (s *S3Base) DeleteObjectListByKeys(bucketName string, objectKeys []string) error {
	var objectIds []types.ObjectIdentifier
	for _, key := range objectKeys {
		objectIds = append(objectIds, types.ObjectIdentifier{Key: aws.String(key)})
	}
	_, err := s.S3Client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &types.Delete{Objects: objectIds},
	})
	if err != nil {
		log.Printf("Couldn't delete objects from bucket %v. Here's why: %v\n", bucketName, err)
	}
	return err
}

func (s *S3Base) UploadPublicFileAcl(bucketName, objectKey, fileName string) error {
	file, err := os.Open(fileName)
	if err != nil {
		log.Printf("Couldn't open file %v to upload. Here's why: %v\n", fileName, err)
	} else {
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {
				log.Fatalf("file close err: %v", err)
			}
		}(file)

		_, err := s.S3Client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   file,
			ACL:    types.ObjectCannedACLPublicReadWrite,
		})
		if err != nil {
			log.Printf("Couldn't upload file %v to %v:%v. Here's why: %v\n",
				fileName, bucketName, objectKey, err)
		}
	}
	return err
}

func (s *S3Base) PutPublicObjectAcl(bucketName, objectKey string) error {
	input := &s3.PutObjectAclInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		ACL:    types.ObjectCannedACLPublicReadWrite,
	}

	_, err := s.S3Client.PutObjectAcl(context.TODO(), input)
	if err != nil {
		return err
	}
	return nil
}

func (s *S3Base) DeleteObjectByVersion(bucketName, objectKey, versionId string) error {
	input := &s3.DeleteObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId),
	}
	_, err := s.S3Client.DeleteObject(context.TODO(), input)
	if err != nil {
		return err
	}
	return nil
}

func (s *S3Base) GetObjectVersionList(bucketName string) ([]types.ObjectVersion, error) {
	input := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	}
	versions, err := s.S3Client.ListObjectVersions(context.TODO(), input)
	if err != nil {
		return nil, err
	}
	return versions.Versions, nil
}

func (s *S3Base) GetObjectByVersion(bucketName, objectKey, versionId string) (string, error) {
	input := &s3.GetObjectInput{
		Bucket:    &bucketName,
		Key:       &objectKey,
		VersionId: &versionId,
	}
	output, err := s.S3Client.GetObject(context.TODO(), input)
	if err != nil {
		return "", err
	}

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(output.Body)
	if err != nil {
		return "", err
	}
	return buf.String(), err
}
