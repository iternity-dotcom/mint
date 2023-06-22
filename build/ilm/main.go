//  Mint, (C) 2021 Minio, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

// S3 client for testing
var serverEnvCfg = loadEnvConfig()
var s3Client *s3.S3
var tierName string
var minioClient *minio.Client
var maxScannerWaitSeconds = 120

func main() {
	// Output to stdout instead of the default stderr
	log.SetOutput(os.Stdout)
	// create custom formatter
	mintFormatter := mintJSONFormatter{}
	// set custom formatter
	log.SetFormatter(&mintFormatter)
	// log Info or above -- success cases are Info level, failures are Error level
	log.SetLevel(log.InfoLevel)

	waitTimeout := getMaxScannerWaitSeconds()
	if waitTimeout != 0 {
		maxScannerWaitSeconds = waitTimeout
	}

	var err error
	s3Client, err = createS3Client(serverEnvCfg)
	if err != nil {
		failureLog("main", map[string]interface{}{}, time.Now(), "", "Failed to create a session with aws-sdk to connect to minio server.", err).Fatal()
		return
	}

	minioClient, err = minio.New(serverEnvCfg.endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(serverEnvCfg.accessKey, serverEnvCfg.secretKey, ""),
		Secure: serverEnvCfg.secure,
	})
	if err != nil {
		failureLog("main", map[string]interface{}{}, time.Now(), "", "Failed to connect with minio client.", err).Fatal()
		return
	}

	putLcImpl := isPutLifecycleConfigurationImplemented()
	if !putLcImpl {
		ignoreLog("main", map[string]interface{}{}, time.Now(), "PutLifecycleConfiguration is not implemented. Skipping all ILM tests.").Info()
		return
	}

	testExpiry()

	versioningImpl := isPutVersioningConfigurationImplemented()
	if versioningImpl {
		testExpireCurrentVersion()
		testExpireNonCurrentVersions()
		testDeleteExpiredDeleteMarker()
	}

	if serverEnvCfg.remoteTierName == "" {
		ignoreLog("main", map[string]interface{}{}, time.Now(), "No remote tier name given. Therefore ILM-Tiering tests will be skipped. "+
			"Provide env 'REMOTE_TIER_NAME' with name of the configured remote tier to enable ILM-Tiering tests.").Info()
		return
	}
	tierName = serverEnvCfg.remoteTierName

	testTransition()
	testExpireTransitioned()
	testRestore()
	testRestoreMultipart()

	cleanupWg.Wait()
}

var cleanupWg sync.WaitGroup

func addCleanupBucket(bucket string, function string, args map[string]interface{}, startTime time.Time, versioned bool) {
	cleanupWg.Add(1)

	go func() {
		defer cleanupWg.Done()
		if versioned {
			cleanupBucketVersioned(bucket, function, args, startTime)
		} else {
			cleanupBucket(bucket, function, args, startTime)
		}
	}()
}

func cleanupBucket(bucket string, function string, args map[string]interface{}, startTime time.Time) {
	start := time.Now()

	input := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	}

	var err error
	for time.Since(start) < 8*time.Minute {
		err = s3Client.ListObjectsPages(input,
			func(page *s3.ListObjectsOutput, lastPage bool) bool {
				for _, o := range page.Contents {
					input := &s3.DeleteObjectInput{
						Bucket: &bucket,
						Key:    o.Key,
					}
					_, err := s3Client.DeleteObject(input)
					if err != nil {
						return true
					}
				}
				return true
			})

		_, err = s3Client.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String(bucket),
		})

		if err != nil {
			time.Sleep(5 * time.Second)
			continue
		}
		return
	}

	failureLog(function, args, startTime, "", fmt.Sprintf("Unable to cleanup bucket '%s' after ILM tests", bucket), err).Error()
	return
}

func cleanupBucketVersioned(bucket string, function string, args map[string]interface{}, startTime time.Time) {
	start := time.Now()

	input := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	}

	var err error
	for time.Since(start) < 8*time.Minute {
		err = s3Client.ListObjectVersionsPages(input,
			func(page *s3.ListObjectVersionsOutput, lastPage bool) bool {
				for _, v := range page.Versions {
					input := &s3.DeleteObjectInput{
						Bucket:    &bucket,
						Key:       v.Key,
						VersionId: v.VersionId,
					}
					_, err := s3Client.DeleteObject(input)
					if err != nil {
						return true
					}
				}
				for _, v := range page.DeleteMarkers {
					input := &s3.DeleteObjectInput{
						Bucket:    &bucket,
						Key:       v.Key,
						VersionId: v.VersionId,
					}
					_, err := s3Client.DeleteObject(input)
					if err != nil {
						return true
					}
				}
				return true
			})

		_, err = s3Client.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String(bucket),
		})

		if err != nil {
			time.Sleep(5 * time.Second)
			continue
		}
		return
	}

	failureLog(function, args, startTime, "", fmt.Sprintf("Unable to cleanup versioned bucket '%s' after ILM tests", bucket), err).Error()
	return
}

func isPutLifecycleConfigurationImplemented() bool {
	bucket := uniqueBucketName()
	startTime := time.Now()
	function := "isPutLifecycleConfigurationImplemented"
	args := map[string]interface{}{
		"bucketName": bucket,
	}
	defer addCleanupBucket(bucket, function, args, startTime, false)

	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return false
	}

	_, err = s3Client.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					ID:     aws.String("checkilmimplemented"),
					Status: aws.String("Enabled"),
					Expiration: &s3.LifecycleExpiration{
						Date: aws.Time(time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, 1)),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String(""),
					},
				},
			},
		},
	})

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "NotImplemented" {
				return false
			}
		}
	}

	return true
}

func isPutVersioningConfigurationImplemented() bool {
	startTime := time.Now()
	function := "isPutVersioningConfigurationImplemented"
	bucket := uniqueBucketName()
	args := map[string]interface{}{
		"bucketName": bucket,
	}

	defer addCleanupBucket(bucket, function, args, startTime, false)

	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return false
	}

	putVersioningInput := &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &s3.VersioningConfiguration{
			Status: aws.String("Enabled"),
		},
	}

	_, err = s3Client.PutBucketVersioning(putVersioningInput)
	if err != nil {
		return false
	}

	return true
}
