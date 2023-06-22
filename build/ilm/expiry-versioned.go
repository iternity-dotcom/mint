/*
*
*  Mint, (C) 2021 Minio, Inc.
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software

*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*
 */

package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	minio "github.com/minio/minio-go/v7"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Tests ilm expiration rules with versioned buckets

func testExpireCurrentVersion() {
	lConfigPast := &s3.BucketLifecycleConfiguration{
		Rules: []*s3.LifecycleRule{
			{
				ID:     aws.String("expirydateinpast"),
				Status: aws.String("Enabled"),
				Expiration: &s3.LifecycleExpiration{
					Date: aws.Time(time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -2)),
				},
				Filter: &s3.LifecycleRuleFilter{
					Prefix: aws.String(""),
				},
			},
		},
	}

	// initialize logging params
	startTime := time.Now()
	function := "testExpireCurrentVersion"
	bucketName := uniqueBucketName()
	objectName := "object"
	contents := []string{"my content 1", "my content 2"}
	args := map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
	}
	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "CreateBucket Failed", err).Error()
		return
	}
	defer addCleanupBucket(bucketName, function, args, startTime, true)

	putVersioningInput := &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &s3.VersioningConfiguration{
			Status: aws.String("Enabled"),
		},
	}

	_, err = s3Client.PutBucketVersioning(putVersioningInput)
	if err != nil {
		failureLog(function, args, startTime, "", "Put VersioningConfiguration failed", err).Error()
		return
	}

	putOutputs := make([]*s3.PutObjectOutput, 0, len(contents))
	for i, content := range contents {
		putInput := &s3.PutObjectInput{
			Body:   aws.ReadSeekCloser(strings.NewReader(content)),
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectName),
		}
		putOutput, err := s3Client.PutObject(putInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("PUT (%d) expected to succeed but failed", i), err).Error()
			return
		}

		putOutputs = append(putOutputs, putOutput)
	}

	_, err = s3Client.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket:                 aws.String(bucketName),
		LifecycleConfiguration: lConfigPast,
	})
	if err != nil {
		failureLog(function, args, startTime, "", "Put LifecycleConfiguration failed", err).Error()
		return
	}

	getInput := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}

	// trigger lifecycle and wait
	_, _ = s3Client.GetObject(getInput)
	time.Sleep(time.Second)

	_, err = s3Client.GetObject(getInput)
	if err == nil {
		failureLog(function, args, startTime, "", "Expected current object version to be deleted", nil).Error()
		return
	}
	aerr, ok := err.(awserr.Error)
	if !ok {
		failureLog(function, args, startTime, "", "Unexpected non aws error on GetObject", err).Error()
		return
	}
	if aerr.Code() != "NoSuchKey" {
		failureLog(function, args, startTime, "", "Unexpected aws error on GetObject", err).Error()
		return
	}

	// Get the older versions, make sure it is preserved
	for i, output := range putOutputs {
		getVersionInput := &s3.GetObjectInput{
			Bucket:    aws.String(bucketName),
			Key:       aws.String(objectName),
			VersionId: aws.String(*output.VersionId),
		}

		getVersionResult, err := s3Client.GetObject(getVersionInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("GetObject (%d) expected to succeed but failed.", i), err).Error()
			return
		}

		body, err := ioutil.ReadAll(getVersionResult.Body)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("GetObject (%d) expected to return data but failed.", i), err).Error()
			return
		}
		_ = getVersionResult.Body.Close()

		if string(body) != contents[i] {
			failureLog(function, args, startTime, "", fmt.Sprintf("GetObject (%d) unexpected body content", i), nil).Error()
			return
		}
	}

	successLogger(function, args, startTime).Info()
}

type nCTestCase struct {
	nonCurrentDaysCfg          *int64
	newerNonCurrentVersionsCfg *int64
	objects                    []struct {
		content     string
		isCurrent   bool
		expDeletion bool
		modTime     time.Time
	}
}

func testExpireNonCurrentVersions() {
	now := time.Now().UTC()
	testCases := []nCTestCase{
		// Testcase 0 - current does not get deleted, the oldest 3 non current objects get deleted
		{
			nonCurrentDaysCfg: aws.Int64(1),
			objects: []struct {
				content     string
				isCurrent   bool
				expDeletion bool
				modTime     time.Time
			}{
				{
					content:     "my content 1",
					isCurrent:   false,
					expDeletion: true,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, -5),
				},
				{
					content:     "my content 2",
					isCurrent:   false,
					expDeletion: true,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, -4),
				},
				{
					content:     "my content 3",
					isCurrent:   false,
					expDeletion: true,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, -3),
				},
				{
					content:     "my content 4",
					isCurrent:   false,
					expDeletion: false,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, -2),
				},
				{
					content:     "my content 5",
					isCurrent:   false,
					expDeletion: false,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, -1),
				},
				{
					content:     "my content 6",
					isCurrent:   true,
					expDeletion: false,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, 0),
				},
			},
		},

		// Testcase 1 - Like in Testcase 0 3 of 5 non current versions should be deleted
		// but due to the 'NewerNoncurrentVersions' set to 4 the latest 4 non current versions are kept and only the
		// oldest one will be deleted
		{
			nonCurrentDaysCfg:          aws.Int64(1),
			newerNonCurrentVersionsCfg: aws.Int64(4),
			objects: []struct {
				content     string
				isCurrent   bool
				expDeletion bool
				modTime     time.Time
			}{
				{
					content:     "my content 1",
					isCurrent:   false,
					expDeletion: true,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, -5),
				},
				{
					content:     "my content 2",
					isCurrent:   false,
					expDeletion: false,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, -4),
				},
				{
					content:     "my content 3",
					isCurrent:   false,
					expDeletion: false,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, -3),
				},
				{
					content:     "my content 4",
					isCurrent:   false,
					expDeletion: false,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, -2),
				},
				{
					content:     "my content 5",
					isCurrent:   false,
					expDeletion: false,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, -1),
				},
				{
					content:     "my content 6",
					isCurrent:   true,
					expDeletion: false,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, 0),
				},
			},
		},

		// Testcase 2 - Only keeps the 3 latest non current versions.
		{
			newerNonCurrentVersionsCfg: aws.Int64(3),
			objects: []struct {
				content     string
				isCurrent   bool
				expDeletion bool
				modTime     time.Time
			}{
				{
					content:     "my content 1",
					isCurrent:   false,
					expDeletion: true,
					modTime:     now.Add(-5 * time.Second),
				},
				{
					content:     "my content 2",
					isCurrent:   false,
					expDeletion: true,
					modTime:     now.Add(-4 * time.Second),
				},
				{
					content:     "my content 3",
					isCurrent:   false,
					expDeletion: false,
					modTime:     now.Add(-3 * time.Second),
				},
				{
					content:     "my content 4",
					isCurrent:   false,
					expDeletion: false,
					modTime:     now.Add(-2 * time.Second),
				},
				{
					content:     "my content 5",
					isCurrent:   false,
					expDeletion: false,
					modTime:     now.Add(-1 * time.Second),
				},
				{
					content:     "my content 6",
					isCurrent:   true,
					expDeletion: false,
					modTime:     now,
				},
			},
		},

		// Testcase 3 - all non current get deleted
		{
			nonCurrentDaysCfg: aws.Int64(1),
			objects: []struct {
				content     string
				isCurrent   bool
				expDeletion bool
				modTime     time.Time
			}{
				{
					content:     "my content 1",
					isCurrent:   false,
					expDeletion: true,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, -5),
				},
				{
					content:     "my content 2",
					isCurrent:   false,
					expDeletion: true,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, -4),
				},
				{
					content:     "my content 3",
					isCurrent:   false,
					expDeletion: true,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, -3),
				},
				{
					content:     "my content 4",
					isCurrent:   true,
					expDeletion: false,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, -2),
				},
			},
		},

		// Testcase 4 - outdated current does not get deleted
		{
			nonCurrentDaysCfg: aws.Int64(1),
			objects: []struct {
				content     string
				isCurrent   bool
				expDeletion bool
				modTime     time.Time
			}{
				{
					content:     "my content 1",
					isCurrent:   false,
					expDeletion: true,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, -8),
				},
				{
					content:     "my content 2",
					isCurrent:   false,
					expDeletion: true,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, -7),
				},
				{
					content:     "my content 3",
					isCurrent:   false,
					expDeletion: true,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, -6),
				},
				{
					content:     "my content 4",
					isCurrent:   true,
					expDeletion: false,
					modTime:     now.Truncate(24*time.Hour).AddDate(0, 0, -5),
				},
			},
		},
	}

	var testWg sync.WaitGroup
	testWg.Add(len(testCases))
	for i, testCase := range testCases {
		go func(idx int, tCase nCTestCase) {
			execTestExpireNonCurrentVersions(idx, tCase)
			defer testWg.Done()
		}(i, testCase)
	}
	testWg.Wait()
}

func execTestExpireNonCurrentVersions(testIdx int, testCase nCTestCase) {
	lConfigPast := &s3.BucketLifecycleConfiguration{
		Rules: []*s3.LifecycleRule{
			{
				ID:     aws.String("expirynoncurrent"),
				Status: aws.String("Enabled"),
				NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
					NoncurrentDays:          testCase.nonCurrentDaysCfg,
					NewerNoncurrentVersions: testCase.newerNonCurrentVersionsCfg,
				},
				Filter: &s3.LifecycleRuleFilter{
					Prefix: aws.String(""),
				},
			},
		},
	}

	// initialize logging params
	startTime := time.Now()
	function := "testExpireNonCurrentVersions"
	bucketName := uniqueBucketName()
	objectName := "object"

	args := map[string]interface{}{
		"testCase":   testIdx,
		"bucketName": bucketName,
		"objectName": objectName,
	}
	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "CreateBucket Failed", err).Error()
		return
	}
	defer addCleanupBucket(bucketName, function, args, startTime, true)

	putVersioningInput := &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &s3.VersioningConfiguration{
			Status: aws.String("Enabled"),
		},
	}
	_, err = s3Client.PutBucketVersioning(putVersioningInput)
	if err != nil {
		failureLog(function, args, startTime, "", "Put VersioningConfiguration failed", err).Error()
		return
	}

	putResults := make([]minio.UploadInfo, 0, len(testCase.objects))
	for i, object := range testCase.objects {
		putResult, err := minioClient.PutObject(
			context.Background(),
			bucketName,
			objectName,
			strings.NewReader(object.content),
			int64(len(object.content)),
			minio.PutObjectOptions{
				Internal: minio.AdvancedPutOptions{
					SourceMTime: object.modTime,
				},
			},
		)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("PUT (%d) expected to succeed but failed", i), err).Error()
			return
		}

		putResults = append(putResults, putResult)
	}

	_, err = s3Client.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket:                 aws.String(bucketName),
		LifecycleConfiguration: lConfigPast,
	})
	if err != nil {
		failureLog(function, args, startTime, "", "Put LifecycleConfiguration failed", err).Error()
		return
	}

	// this feature relies on the datascanner, so we have to wait for it
	if testCase.newerNonCurrentVersionsCfg != nil {
		time.Sleep(time.Duration(maxScannerWaitSeconds) * time.Second)
	}

	for i, object := range testCase.objects {
		getVersionedInput := &s3.GetObjectInput{
			Bucket:    aws.String(bucketName),
			Key:       aws.String(objectName),
			VersionId: aws.String(putResults[i].VersionID),
		}

		// trigger lifecycle and wait
		if testCase.newerNonCurrentVersionsCfg == nil {
			_, _ = s3Client.GetObject(getVersionedInput)
			time.Sleep(time.Second)
		}

		_, err = s3Client.GetObject(getVersionedInput)
		objectFound := true
		if err != nil {
			aerr, ok := err.(awserr.Error)
			if !ok {
				failureLog(function, args, startTime, "", fmt.Sprintf("Unexpected non aws error on GetObject (%d)", i), err).Error()
				return
			}
			if aerr.Code() != "NoSuchVersion" {
				failureLog(function, args, startTime, "", fmt.Sprintf("Unexpected aws error on GetObject (%d)", i), err).Error()
				return
			}
			objectFound = false
		}

		if !object.expDeletion && !objectFound {
			failureLog(function, args, startTime, "", fmt.Sprintf("Expected object version (%d) to be found.", i), err).Error()
			return
		}

		if object.expDeletion && objectFound {
			failureLog(function, args, startTime, "", fmt.Sprintf("Expected object version (%d) to be deleted", i), nil).Error()
			return
		}
	}

	getVerInput := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	}

	listVerResult, err := s3Client.ListObjectVersions(getVerInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("ListObjectVersions expected to succeed but got %v", err), err).Error()
		return
	}

	if len(listVerResult.DeleteMarkers) != 0 {
		failureLog(function, args, startTime, "", "Expected ListObjectVersions to no DeleteMarkers.", nil).Error()
		return
	}

	currentVerIdx := 0
	expVersionsIdx := make([]int, 0)
	for i, object := range testCase.objects {
		if !object.expDeletion {
			expVersionsIdx = append(expVersionsIdx, i)
		}
		if object.isCurrent {
			currentVerIdx = i
		}
	}

	if len(listVerResult.Versions) != len(expVersionsIdx) {
		failureLog(function, args, startTime, "", fmt.Sprintf("Expected ListObjectVersions to return (%d) versions, but (%d) were returned.", len(expVersionsIdx), len(listVerResult.Versions)), nil).Error()
		return
	}

	currentVer := listVerResult.Versions[0].VersionId
	if currentVer == nil {
		failureLog(function, args, startTime, "", "Expected current versionId to be not empty.", nil).Error()
		return
	}

	if putResults[currentVerIdx].VersionID != *currentVer {
		failureLog(function, args, startTime, "", fmt.Sprintf("Expected current version to be %s.", putResults[currentVerIdx].VersionID), nil).Error()
		return
	}

	successLogger(function, args, startTime).Info()
}

func testDeleteExpiredDeleteMarker() {
	lConfigPast := &s3.BucketLifecycleConfiguration{
		Rules: []*s3.LifecycleRule{
			{
				ID:     aws.String("expirydeletemarkers"),
				Status: aws.String("Enabled"),
				Expiration: &s3.LifecycleExpiration{
					ExpiredObjectDeleteMarker: aws.Bool(true),
				},
				NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
					NoncurrentDays: aws.Int64(1),
				},
				Filter: &s3.LifecycleRuleFilter{
					Prefix: aws.String(""),
				},
			},
		},
	}

	// initialize logging params
	startTime := time.Now()
	function := "testDeleteExpiredDeleteMarker"
	bucketName := uniqueBucketName()
	objectName := "object"
	objectContent := "object content"

	args := map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
	}
	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "CreateBucket Failed", err).Error()
		return
	}
	defer addCleanupBucket(bucketName, function, args, startTime, true)

	putVersioningInput := &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &s3.VersioningConfiguration{
			Status: aws.String("Enabled"),
		},
	}
	_, err = s3Client.PutBucketVersioning(putVersioningInput)
	if err != nil {
		failureLog(function, args, startTime, "", "Put VersioningConfiguration failed", err).Error()
		return
	}

	putResult, err := minioClient.PutObject(
		context.Background(),
		bucketName,
		objectName,
		strings.NewReader(objectContent),
		int64(len(objectContent)),
		minio.PutObjectOptions{
			Internal: minio.AdvancedPutOptions{
				SourceMTime: time.Now().AddDate(0, 0, -10), // old enough to be removed by lifecycle rule
			},
		},
	)
	if err != nil {
		failureLog(function, args, startTime, "", "PUT expected to succeed but failed", err).Error()
		return
	}

	err = minioClient.RemoveObject(context.Background(), bucketName, objectName, minio.RemoveObjectOptions{
		Internal: minio.AdvancedRemoveOptions{
			ReplicationMTime: time.Now().AddDate(0, 0, -10), // old enough to be removed by lifecycle rule
		},
	})
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Error()
		return
	}

	_, err = s3Client.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket:                 aws.String(bucketName),
		LifecycleConfiguration: lConfigPast,
	})
	if err != nil {
		failureLog(function, args, startTime, "", "Put LifecycleConfiguration failed", err).Error()
		return
	}

	getVersionedInput := &s3.GetObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectName),
		VersionId: aws.String(putResult.VersionID),
	}

	// trigger lifecycle to expire all non current versions - after this get the delete marker is an expired delete marker
	_, _ = s3Client.GetObject(getVersionedInput)
	time.Sleep(time.Second)

	getVerInput := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	}

	waitTime := 0
	var listVerResult *s3.ListObjectVersionsOutput
	for waitTime < maxScannerWaitSeconds {
		listVerResult, err = s3Client.ListObjectVersions(getVerInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("ListObjectVersions expected to succeed but got %v", err), err).Error()
			return
		}

		if len(listVerResult.Versions) != 0 {
			failureLog(function, args, startTime, "", "Expected to return 0 versions.", nil).Error()
			return
		}

		if len(listVerResult.DeleteMarkers) == 0 {
			break
		}

		waitTime += 5
		time.Sleep(5 * time.Second)
	}

	if len(listVerResult.DeleteMarkers) != 0 {
		failureLog(function, args, startTime, "", "Expected ListObjectVersions to return no DeleteMarker.", nil).Error()
		return
	}

	successLogger(function, args, startTime).Info()
}
