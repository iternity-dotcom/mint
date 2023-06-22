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
	"bytes"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Tests ilm restore object
func testRestore() {
	// initialize logging params
	startTime := time.Now()
	function := "testRestore"
	bucketName := uniqueBucketName()
	objectName := "object"
	args := map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
	}

	lConfigTransition := &s3.BucketLifecycleConfiguration{
		Rules: []*s3.LifecycleRule{
			{
				ID:     aws.String("transitiondateinpast"),
				Status: aws.String("Enabled"),
				Transitions: []*s3.Transition{
					{
						Date:         aws.Time(time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -2)),
						StorageClass: aws.String(tierName),
					},
				},
				Filter: &s3.LifecycleRuleFilter{
					Prefix: aws.String(""),
				},
			},
		},
	}

	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "CreateBucket Failed", err).Error()
		return
	}
	defer addCleanupBucket(bucketName, function, args, startTime, false)

	_, err = s3Client.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket:                 aws.String(bucketName),
		LifecycleConfiguration: lConfigTransition,
	})
	if err != nil {
		failureLog(function, args, startTime, "", "Put LifecycleConfiguration for transitioning failed", err).Error()
		return
	}

	putInput := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader("my content 1")),
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}
	_, err = s3Client.PutObject(putInput)
	if err != nil {
		failureLog(function, args, startTime, "", "PUT expected to succeed but failed", err).Error()
		return
	}

	getInput := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}

	// wait some time before getting object the first time
	// transition is an async process
	time.Sleep(1 * time.Second)

	// get with 3 retries
	var result *s3.GetObjectOutput
	for i := 0; i < 3; i++ {
		result, err = s3Client.GetObject(getInput)
		if err != nil {
			failureLog(function, args, startTime, "", "GET expected to succeed but failed", err).Error()
			return
		}

		if result.StorageClass != nil && *(result.StorageClass) == tierName {
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	if result.StorageClass == nil || *(result.StorageClass) != tierName {
		failureLog(function, args, startTime, "", "Expected object to be transitioned.", nil).Error()
		return
	}

	if result.Restore != nil {
		failureLog(function, args, startTime, "", "Expected restore header to be empty.", nil).Error()
		return
	}

	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		failureLog(function, args, startTime, "", "Expected to return data but failed", err).Error()
		return
	}
	_ = result.Body.Close()

	if string(body) != "my content 1" {
		failureLog(function, args, startTime, "", "Unexpected body content", err).Error()
		return
	}

	_, err = s3Client.RestoreObject(&s3.RestoreObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
		RestoreRequest: &s3.RestoreRequest{
			Days: aws.Int64(1),
		},
	})
	if err != nil {
		failureLog(function, args, startTime, "", "Restore object failed", err).Error()
		return
	}

	getInputAfterRestore := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}

	// get with 5 retries
	var resultAfterRestore *s3.GetObjectOutput
	for i := 0; i < 5; i++ {
		resultAfterRestore, err = s3Client.GetObject(getInputAfterRestore)
		if err != nil {
			continue
		}

		if resultAfterRestore.Restore == nil {
			continue
		}

		time.Sleep(time.Second)
	}

	if err != nil {
		failureLog(function, args, startTime, "", "Failed to get object after restore", nil).Error()
		return
	}

	if resultAfterRestore.Restore == nil {
		failureLog(function, args, startTime, "", "Expected restore header to be set.", nil).Error()
		return
	}

	restoreHeader := *resultAfterRestore.Restore
	var restoreRegex = regexp.MustCompile(`ongoing-request="(.*?)"(, expiry-date="(.*?)")?`)
	matches := restoreRegex.FindStringSubmatch(restoreHeader)
	if len(matches) != 4 {
		failureLog(function, args, startTime, "", "Expected restore header contain ongoing-request status and expiry-date.", nil).Error()
		return
	}

	if matches[1] != "false" {
		failureLog(function, args, startTime, "", "Expected status in restore header should be 'false'.", nil).Error()
		return
	}

	expiry, err := time.Parse(http.TimeFormat, matches[3])
	if err != nil {
		failureLog(function, args, startTime, "", "Expected 'expiry-date' cannot be parsed.", err).Error()
		return
	}

	if expiry != time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, 2) {
		failureLog(function, args, startTime, "", "Expected 'expiry-date' should be mignight in 2 days.", nil).Error()
		return
	}

	bodyAfterRestore, err := ioutil.ReadAll(resultAfterRestore.Body)
	if err != nil {
		failureLog(function, args, startTime, "", "Expected to return data after restore but failed", err).Error()
		return
	}
	_ = resultAfterRestore.Body.Close()

	if string(bodyAfterRestore) != "my content 1" {
		failureLog(function, args, startTime, "", "Unexpected body content after restore", err).Error()
		return
	}

	successLogger(function, args, startTime).Info()
}

// Tests ilm restore object for multipart
func testRestoreMultipart() {
	// initialize logging params
	startTime := time.Now()
	function := "testRestoreMultipart"
	bucketName := uniqueBucketName()
	objectName := "object"
	args := map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
	}

	lConfigTransition := &s3.BucketLifecycleConfiguration{
		Rules: []*s3.LifecycleRule{
			{
				ID:     aws.String("transitiondateinpast"),
				Status: aws.String("Enabled"),
				Transitions: []*s3.Transition{
					{
						Date:         aws.Time(time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -2)),
						StorageClass: aws.String(tierName),
					},
				},
				Filter: &s3.LifecycleRuleFilter{
					Prefix: aws.String(""),
				},
			},
		},
	}

	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "CreateBucket Failed", err).Error()
		return
	}
	defer addCleanupBucket(bucketName, function, args, startTime, false)

	_, err = s3Client.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket:                 aws.String(bucketName),
		LifecycleConfiguration: lConfigTransition,
	})
	if err != nil {
		failureLog(function, args, startTime, "", "Put LifecycleConfiguration for transitioning failed", err).Error()
		return
	}

	fileSize := 15 * 1024 * 1024
	createTestfile(int64(fileSize), objectName)

	f, err := os.Open(objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "Open testfile failed", err).Error()
		return
	}
	defer os.Remove(objectName)

	partSize := 5 * 1024 * 1024 // Set part size to 5 MB (minimum size for a part)
	partCount := fileSize / partSize
	parts := make([]*string, partCount)
	inputFileBuffer := make([]byte, fileSize)

	_, err = f.Read(inputFileBuffer)
	if err != nil {
		failureLog(function, args, startTime, "", "Reading file failed", err).Error()
		return
	}

	err = f.Close()
	if err != nil {
		failureLog(function, args, startTime, "", "Failed to close file after reading", err).Error()
		return
	}

	multipartUpload, err := s3Client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	})

	if err != nil {
		failureLog(function, args, startTime, "", "CreateMultipartupload API failed", err).Error()
		return
	}

	for j := 0; j < partCount; j++ {
		result, errUpload := s3Client.UploadPart(&s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectName),
			UploadId:   multipartUpload.UploadId,
			PartNumber: aws.Int64(int64(j + 1)),
			Body:       bytes.NewReader(inputFileBuffer[j*partSize : (j+1)*partSize]),
		})
		if errUpload != nil {
			_, _ = s3Client.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucketName),
				Key:      aws.String(objectName),
				UploadId: multipartUpload.UploadId,
			})
			failureLog(function, args, startTime, "", "UploadPart API failed for", errUpload).Error()
			return
		}
		parts[j] = result.ETag
	}

	completedParts := make([]*s3.CompletedPart, len(parts))
	for i, part := range parts {
		completedParts[i] = &s3.CompletedPart{
			ETag:       part,
			PartNumber: aws.Int64(int64(i + 1)),
		}
	}

	_, err = s3Client.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts},
		UploadId: multipartUpload.UploadId,
	})
	if err != nil {
		failureLog(function, args, startTime, "", "CompleteMultipartUpload is expected to succeed but failed", nil).Error()
		return
	}

	getInput := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}

	// wait some time before getting object the first time
	// transition is an async process
	time.Sleep(1 * time.Second)

	// get with 3 retries
	var result *s3.GetObjectOutput
	for i := 0; i < 3; i++ {
		result, err = s3Client.GetObject(getInput)
		if err != nil {
			failureLog(function, args, startTime, "", "GET expected to succeed but failed", err).Error()
			return
		}

		defer result.Body.Close() // fixed number of loops

		if result.StorageClass != nil && *(result.StorageClass) == tierName {
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	if result.StorageClass == nil || *(result.StorageClass) != tierName {
		failureLog(function, args, startTime, "", "Expected object to be transitioned.", nil).Error()
		return
	}

	if result.Restore != nil {
		failureLog(function, args, startTime, "", "Expected restore header to be empty.", nil).Error()
		return
	}

	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		failureLog(function, args, startTime, "", "Expected to return data but failed", err).Error()
		return
	}
	_ = result.Body.Close()

	if !bytes.Equal(body, inputFileBuffer) {
		failureLog(function, args, startTime, "", "Unexpected body content after transition", err).Error()
		return
	}

	_, err = s3Client.RestoreObject(&s3.RestoreObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
		RestoreRequest: &s3.RestoreRequest{
			Days: aws.Int64(1),
		},
	})
	if err != nil {
		failureLog(function, args, startTime, "", "Restore object failed", err).Error()
		return
	}

	getInputAfterRestore := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}

	// get with 5 retries
	var resultAfterRestore *s3.GetObjectOutput
	for i := 0; i < 5; i++ {
		resultAfterRestore, err = s3Client.GetObject(getInputAfterRestore)
		if err != nil {
			continue
		}

		defer resultAfterRestore.Body.Close() // fixed number of loops

		if resultAfterRestore.Restore == nil {
			continue
		}

		time.Sleep(time.Second)
	}

	if err != nil {
		failureLog(function, args, startTime, "", "Failed to get object after restore", nil).Error()
		return
	}

	if resultAfterRestore.Restore == nil {
		failureLog(function, args, startTime, "", "Expected restore header to be set.", nil).Error()
		return
	}

	restoreHeader := *resultAfterRestore.Restore
	var restoreRegex = regexp.MustCompile(`ongoing-request="(.*?)"(, expiry-date="(.*?)")?`)
	matches := restoreRegex.FindStringSubmatch(restoreHeader)
	if len(matches) != 4 {
		failureLog(function, args, startTime, "", "Expected restore header contain ongoing-request status and expiry-date.", nil).Error()
		return
	}

	if matches[1] != "false" {
		failureLog(function, args, startTime, "", "Expected status in restore header should be 'false'.", nil).Error()
		return
	}

	expiry, err := time.Parse(http.TimeFormat, matches[3])
	if err != nil {
		failureLog(function, args, startTime, "", "Expected 'expiry-date' cannot be parsed.", err).Error()
		return
	}

	if expiry != time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, 2) {
		failureLog(function, args, startTime, "", "Expected 'expiry-date' should be mignight in 2 days.", nil).Error()
		return
	}

	bodyAfterRestore, err := ioutil.ReadAll(resultAfterRestore.Body)
	if err != nil {
		failureLog(function, args, startTime, "", "Expected to return data after restore but failed", err).Error()
		return
	}
	_ = resultAfterRestore.Body.Close()

	if !bytes.Equal(bodyAfterRestore, inputFileBuffer) {
		failureLog(function, args, startTime, "", "Unexpected body content after restore", err).Error()
		return
	}

	successLogger(function, args, startTime).Info()
}
