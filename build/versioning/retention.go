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
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Test locking retention governance
func testLockingRetentionGovernance() {
	startTime := time.Now()
	function := "testLockingRetentionGovernance"
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "versioning-test-")
	object := "testObject"
	expiry := 1 * time.Minute
	args := map[string]interface{}{
		"bucketName": bucket,
		"objectName": object,
		"expiry":     expiry,
	}
	ctx := context.Background()

	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket:                     aws.String(bucket),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotImplemented: A header you provided implies functionality that is not implemented") {
			ignoreLog(function, args, startTime, "Versioning is not implemented").Info()
			return
		}
		failureLog(function, args, startTime, "", "CreateBucket failed", err).Fatal()
		return
	}
	defer cleanupBucket(bucket, function, args, startTime)

	type uploadedObject struct {
		retention        string
		retentionUntil   time.Time
		successfulRemove bool
		versionId        string
		deleteMarker     bool
	}

	uploads := []uploadedObject{
		{},
		{retention: "GOVERNANCE", retentionUntil: time.Now().UTC().Add(time.Hour)},
		{},
	}

	// Upload versions and save their version IDs
	for i := range uploads {
		putInput := &s3.PutObjectInput{
			Body:   strings.NewReader("content"),
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
		}
		if uploads[i].retention != "" {
			putInput.ObjectLockMode = types.ObjectLockMode(uploads[i].retention)
			putInput.ObjectLockRetainUntilDate = aws.Time(uploads[i].retentionUntil)

		}
		output, err := s3Client.PutObject(ctx, putInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("PUT expected to succeed but got %v", err), err).Fatal()
			return
		}
		uploads[i].versionId = *output.VersionId
	}

	// In all cases, we can remove an object by creating a delete marker
	// First delete without version ID
	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	deleteOutput, err := s3Client.DeleteObject(ctx, deleteInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("DELETE expected to succeed but got %v", err), err).Fatal()
		return
	}

	uploads = append(uploads, uploadedObject{versionId: *deleteOutput.VersionId, deleteMarker: true})

	// Put tagging on each version
	for i := range uploads {
		if uploads[i].deleteMarker {
			continue
		}
		deleteInput := &s3.DeleteObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			VersionId: aws.String(uploads[i].versionId),
		}
		_, err = s3Client.DeleteObject(ctx, deleteInput)
		if err == nil && uploads[i].retention != "" {
			failureLog(function, args, startTime, "", "DELETE expected to fail but succeed instead", nil).Fatal()
			return
		}
		if err != nil && uploads[i].retention == "" {
			failureLog(function, args, startTime, "", fmt.Sprintf("DELETE expected to succeed but got %v", err), err).Fatal()
			return
		}
	}

	successLogger(function, args, startTime).Info()
}

// Test locking retention compliance
func testLockingRetentionCompliance() {
	startTime := time.Now()
	function := "testLockingRetentionCompliance"
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "versioning-test-")
	object := "testObject"
	expiry := 1 * time.Minute
	args := map[string]interface{}{
		"bucketName": bucket,
		"objectName": object,
		"expiry":     expiry,
	}
	ctx := context.Background()

	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket:                     aws.String(bucket),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotImplemented: A header you provided implies functionality that is not implemented") {
			ignoreLog(function, args, startTime, "Versioning is not implemented").Info()
			return
		}
		failureLog(function, args, startTime, "", "CreateBucket failed", err).Fatal()
		return
	}

	defer cleanupBucket(bucket, function, args, startTime)

	type uploadedObject struct {
		retention        string
		retentionUntil   time.Time
		successfulRemove bool
		versionId        string
		deleteMarker     bool
	}

	uploads := []uploadedObject{
		{},
		{retention: "COMPLIANCE", retentionUntil: time.Now().UTC().Add(time.Minute)},
		{},
	}

	// Upload versions and save their version IDs
	for i := range uploads {
		putInput := &s3.PutObjectInput{
			Body:   strings.NewReader("content"),
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
		}
		if uploads[i].retention != "" {
			putInput.ObjectLockMode = types.ObjectLockMode(uploads[i].retention)
			putInput.ObjectLockRetainUntilDate = aws.Time(uploads[i].retentionUntil)

		}
		output, err := s3Client.PutObject(ctx, putInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("PUT expected to succeed but got %v", err), err).Fatal()
			return
		}
		uploads[i].versionId = *output.VersionId
	}

	// In all cases, we can remove an object by creating a delete marker
	// First delete without version ID
	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	deleteOutput, err := s3Client.DeleteObject(ctx, deleteInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("DELETE expected to succeed but got %v", err), err).Fatal()
		return
	}

	uploads = append(uploads, uploadedObject{versionId: *deleteOutput.VersionId, deleteMarker: true})

	// Put tagging on each version
	for i := range uploads {
		if uploads[i].deleteMarker {
			continue
		}
		deleteInput := &s3.DeleteObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			VersionId: aws.String(uploads[i].versionId),
		}
		_, err = s3Client.DeleteObject(ctx, deleteInput)
		if err == nil && uploads[i].retention != "" {
			failureLog(function, args, startTime, "", "DELETE expected to fail but succeed instead", nil).Fatal()
			return
		}
		if err != nil && uploads[i].retention == "" {
			failureLog(function, args, startTime, "", fmt.Sprintf("DELETE expected to succeed but got %v", err), err).Fatal()
			return
		}
	}

	successLogger(function, args, startTime).Info()
}

// Test locking retention compliance - latest version retention
func testLockingRetentionComplianceLatestVersionRetention() {
	startTime := time.Now()
	function := "testLockingRetentionComplianceLatestVersionRetention"
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "versioning-test-")
	object := "testObject"
	expiry := 1 * time.Minute
	args := map[string]interface{}{
		"bucketName": bucket,
		"objectName": object,
		"expiry":     expiry,
	}

	ctx := context.Background()

	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket:                     aws.String(bucket),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotImplemented: A header you provided implies functionality that is not implemented") {
			ignoreLog(function, args, startTime, "Versioning is not implemented").Info()
			return
		}
		failureLog(function, args, startTime, "", "CreateBucket failed", err).Fatal()
		return
	}

	defer cleanupBucket(bucket, function, args, startTime)

	type uploadedObject struct {
		retention        string
		retentionUntil   time.Time
		successfulRemove bool
		versionId        string
		deleteMarker     bool
	}

	uploads := []uploadedObject{
		{},
		{retention: "COMPLIANCE", retentionUntil: time.Now().UTC().Add(time.Minute)},
	}

	// Upload versions and save their version IDs
	for i := range uploads {
		putInput := &s3.PutObjectInput{
			Body:   strings.NewReader("content"),
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
		}
		if uploads[i].retention != "" {
			putInput.ObjectLockMode = types.ObjectLockMode(uploads[i].retention)
			putInput.ObjectLockRetainUntilDate = aws.Time(uploads[i].retentionUntil)

		}
		output, err := s3Client.PutObject(ctx, putInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("PUT expected to succeed but got %v", err), err).Fatal()
			return
		}
		uploads[i].versionId = *output.VersionId
	}

	// In all cases, we can remove an object by creating a delete marker
	// First delete without version ID
	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	deleteOutput, err := s3Client.DeleteObject(ctx, deleteInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("DELETE expected to succeed but got %v", err), err).Fatal()
		return
	}

	uploads = append(uploads, uploadedObject{versionId: *deleteOutput.VersionId, deleteMarker: true})

	// Put tagging on each version
	for i := range uploads {
		if uploads[i].deleteMarker {
			continue
		}
		deleteInput := &s3.DeleteObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			VersionId: aws.String(uploads[i].versionId),
		}
		_, err = s3Client.DeleteObject(ctx, deleteInput)
		if err == nil && uploads[i].retention != "" {
			failureLog(function, args, startTime, "", "DELETE expected to fail but succeed instead", nil).Fatal()
			return
		}
		if err != nil && uploads[i].retention == "" {
			failureLog(function, args, startTime, "", fmt.Sprintf("DELETE expected to succeed but got %v", err), err).Fatal()
			return
		}
	}

	successLogger(function, args, startTime).Info()
}

func testPutGetDeleteRetentionGovernance() {
	functionName := "testPutGetDeleteRetentionGovernance"
	testPutGetDeleteLockingRetention(functionName, "GOVERNANCE")
}

func testPutGetRetentionCompliance() {
	functionName := "testPutGetRetentionCompliance"
	testPutGetDeleteLockingRetention(functionName, "COMPLIANCE")
}

// Test locking retention governance
func testPutGetDeleteLockingRetention(function, retentionMode string) {
	startTime := time.Now()
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "versioning-test-")
	object := "testObject"
	args := map[string]interface{}{
		"bucketName":    bucket,
		"objectName":    object,
		"retentionMode": retentionMode,
	}
	ctx := context.Background()

	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket:                     aws.String(bucket),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotImplemented: A header you provided implies functionality that is not implemented") {
			ignoreLog(function, args, startTime, "Versioning is not implemented").Info()
			return
		}
		failureLog(function, args, startTime, "", "CreateBucket failed", err).Fatal()
		return
	}

	defer cleanupBucket(bucket, function, args, startTime)

	oneMinuteRetention := time.Now().UTC().Add(time.Minute)
	twoMinutesRetention := oneMinuteRetention.Add(time.Minute)

	// Upload version and save the version ID
	putInput := &s3.PutObjectInput{
		Body:                      strings.NewReader("content"),
		Bucket:                    aws.String(bucket),
		Key:                       aws.String(object),
		ObjectLockMode:            types.ObjectLockMode(retentionMode),
		ObjectLockRetainUntilDate: aws.Time(oneMinuteRetention),
	}

	output, err := s3Client.PutObject(ctx, putInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("PUT expected to succeed but got %v", err), err).Fatal()
		return
	}
	versionId := *output.VersionId

	// Increase retention until date
	putRetentionInput := &s3.PutObjectRetentionInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: aws.String(versionId),
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionMode(retentionMode),
			RetainUntilDate: aws.Time(twoMinutesRetention),
		},
	}
	_, err = s3Client.PutObjectRetention(ctx, putRetentionInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("PutObjectRetention expected to succeed but got %v", err), err).Fatal()
		return
	}

	getRetentionInput := &s3.GetObjectRetentionInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: aws.String(versionId),
	}

	retentionOutput, err := s3Client.GetObjectRetention(ctx, getRetentionInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("GetObjectRetention expected to succeed but got %v", err), err).Fatal()
		return
	}

	// Compare until retention date with truncating precision less than second
	if retentionOutput.Retention.RetainUntilDate.Truncate(time.Second).String() != twoMinutesRetention.Truncate(time.Second).String() {
		failureLog(function, args, startTime, "", "Unexpected until retention date", nil).Fatal()
		return
	}

	// Lower retention until date, should fail
	putRetentionInput = &s3.PutObjectRetentionInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: aws.String(versionId),
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionMode(retentionMode),
			RetainUntilDate: aws.Time(oneMinuteRetention),
		},
	}
	_, err = s3Client.PutObjectRetention(ctx, putRetentionInput)
	if err == nil {
		failureLog(function, args, startTime, "", "PutObjectRetention expected to fail but succeeded", nil).Fatal()
		return
	}

	// Remove retention without governance bypass
	putRetentionInput = &s3.PutObjectRetentionInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: aws.String(versionId),
		Retention: &types.ObjectLockRetention{
			Mode: types.ObjectLockRetentionMode(""),
		},
	}

	_, err = s3Client.PutObjectRetention(ctx, putRetentionInput)
	if err == nil {
		failureLog(function, args, startTime, "", "Operation expected to fail but succeeded", nil).Fatal()
		return
	}

	if retentionMode == "GOVERNANCE" {
		// Remove governance retention without govenance bypass
		putRetentionInput = &s3.PutObjectRetentionInput{
			Bucket:                    aws.String(bucket),
			Key:                       aws.String(object),
			VersionId:                 aws.String(versionId),
			BypassGovernanceRetention: aws.Bool(true),
			Retention: &types.ObjectLockRetention{
				Mode: types.ObjectLockRetentionMode(""),
			},
		}

		_, err = s3Client.PutObjectRetention(ctx, putRetentionInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("Expected to succeed but failed with %v", err), err).Fatal()
			return
		}
	}

	successLogger(function, args, startTime).Info()
}

// Test locking retention governance - latest version retention
func testLockingRetentionGovernanceLatestVersionRetention() {
	startTime := time.Now()
	function := "testLockingRetentionGovernanceLatestVersionRetention"
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "versioning-test-")
	object := "testObject"
	expiry := 1 * time.Minute
	args := map[string]interface{}{
		"bucketName": bucket,
		"objectName": object,
		"expiry":     expiry,
	}
	ctx := context.Background()

	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket:                     aws.String(bucket),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotImplemented: A header you provided implies functionality that is not implemented") {
			ignoreLog(function, args, startTime, "Versioning is not implemented").Info()
			return
		}
		failureLog(function, args, startTime, "", "CreateBucket failed", err).Fatal()
		return
	}
	defer cleanupBucket(bucket, function, args, startTime)

	type uploadedObject struct {
		retention        string
		retentionUntil   time.Time
		successfulRemove bool
		versionId        string
		deleteMarker     bool
	}

	uploads := []uploadedObject{
		{},
		{retention: "GOVERNANCE", retentionUntil: time.Now().UTC().Add(time.Hour)},
	}

	// Upload versions and save their version IDs
	for i := range uploads {
		putInput := &s3.PutObjectInput{
			Body:   strings.NewReader("content"),
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
		}
		if uploads[i].retention != "" {
			putInput.ObjectLockMode = types.ObjectLockMode(uploads[i].retention)
			putInput.ObjectLockRetainUntilDate = aws.Time(uploads[i].retentionUntil)

		}
		output, err := s3Client.PutObject(ctx, putInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("PUT expected to succeed but got %v", err), err).Fatal()
			return
		}
		uploads[i].versionId = *output.VersionId
	}

	// In all cases, we can remove an object by creating a delete marker
	// First delete without version ID
	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	deleteOutput, err := s3Client.DeleteObject(ctx, deleteInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("DELETE expected to succeed but got %v", err), err).Fatal()
		return
	}

	uploads = append(uploads, uploadedObject{versionId: *deleteOutput.VersionId, deleteMarker: true})

	// Put tagging on each version
	for i := range uploads {
		if uploads[i].deleteMarker {
			continue
		}
		deleteInput := &s3.DeleteObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			VersionId: aws.String(uploads[i].versionId),
		}
		_, err = s3Client.DeleteObject(ctx, deleteInput)
		if err == nil && uploads[i].retention != "" {
			failureLog(function, args, startTime, "", "DELETE expected to fail but succeed instead", nil).Fatal()
			return
		}
		if err != nil && uploads[i].retention == "" {
			failureLog(function, args, startTime, "", fmt.Sprintf("DELETE expected to succeed but got %v", err), err).Fatal()
			return
		}
	}

	successLogger(function, args, startTime).Info()
}

// Test locking retention governance (multipart)
func testLockingRetentionGovernanceMultipart() {
	startTime := time.Now()
	function := "testLockingRetentionGovernanceMultipart"
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "versioning-test-")
	object := "testObject"
	expiry := 1 * time.Minute
	args := map[string]interface{}{
		"bucketName": bucket,
		"objectName": object,
		"expiry":     expiry,
	}
	ctx := context.Background()

	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket:                     aws.String(bucket),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotImplemented: A header you provided implies functionality that is not implemented") {
			ignoreLog(function, args, startTime, "Versioning is not implemented").Info()
			return
		}
		failureLog(function, args, startTime, "", "CreateBucket failed", err).Fatal()
		return
	}

	fileSize := 30 * 1024 * 1024
	createTestObject(int64(fileSize), object)

	f, err := os.Open(object)
	if err != nil {
		failureLog(function, args, startTime, "", "Open testobject failed", err).Fatal()
		return
	}
	defer f.Close()
	defer os.Remove(object)

	defer cleanupBucket(bucket, function, args, startTime)

	type uploadedObject struct {
		retention        string
		retentionUntil   time.Time
		successfulRemove bool
		versionId        string
		deleteMarker     bool
	}

	uploads := []uploadedObject{
		{},
		{retention: "GOVERNANCE", retentionUntil: time.Now().UTC().Add(time.Hour)},
		{},
	}

	partSize := 5 * 1024 * 1024 // Set part size to 5 MB (minimum size for a part)

	// Upload versions and save their version IDs
	for i := range uploads {
		cmui := s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
		}

		if uploads[i].retention != "" {
			cmui.ObjectLockMode = types.ObjectLockMode(uploads[i].retention)
			cmui.ObjectLockRetainUntilDate = aws.Time(uploads[i].retentionUntil)
		}

		multipartUpload, err := s3Client.CreateMultipartUpload(ctx, &cmui)

		if err != nil {
			failureLog(function, args, startTime, "", "CreateMultipartupload API failed", err).Fatal()
			return
		}

		filePart := make([]byte, partSize)
		partCount := fileSize / partSize
		parts := make([]*string, partCount)
		for j := 0; j < partCount; j++ {
			_, err := f.ReadAt(filePart, int64(partSize*j))
			if err != nil {
				failureLog(function, args, startTime, "", "ReadAt failed", err).Fatal()
				return
			}
			r := bytes.NewReader(filePart)

			result, errUpload := s3Client.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:     aws.String(bucket),
				Key:        aws.String(object),
				UploadId:   multipartUpload.UploadId,
				PartNumber: aws.Int32(int32(j + 1)),
				Body:       r,
			})
			if errUpload != nil {
				_, _ = s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
					Bucket:   aws.String(bucket),
					Key:      aws.String(object),
					UploadId: multipartUpload.UploadId,
				})
				failureLog(function, args, startTime, "", "UploadPart API failed for", errUpload).Fatal()
				return
			}
			parts[j] = result.ETag
		}

		completedParts := make([]types.CompletedPart, len(parts))
		for i, part := range parts {
			completedParts[i] = types.CompletedPart{
				ETag:       part,
				PartNumber: aws.Int32(int32(i + 1)),
			}
		}

		output, err := s3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: completedParts},
			UploadId: multipartUpload.UploadId,
		})
		if err != nil {
			failureLog(function, args, startTime, "", "CompleteMultipartUpload is expected to succeed but failed", errors.New("expected nil")).Fatal()
			return
		}

		uploads[i].versionId = *output.VersionId
	}

	// In all cases, we can remove an object by creating a delete marker
	// First delete without version ID
	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	deleteOutput, err := s3Client.DeleteObject(ctx, deleteInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("DELETE expected to succeed but got %v", err), err).Fatal()
		return
	}

	uploads = append(uploads, uploadedObject{versionId: *deleteOutput.VersionId, deleteMarker: true})

	// Put tagging on each version
	for i := range uploads {
		if uploads[i].deleteMarker {
			continue
		}
		deleteInput := &s3.DeleteObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			VersionId: aws.String(uploads[i].versionId),
		}
		_, err = s3Client.DeleteObject(ctx, deleteInput)
		if err == nil && uploads[i].retention != "" {
			failureLog(function, args, startTime, "", "DELETE expected to fail but succeed instead", nil).Fatal()
			return
		}
		if err != nil && uploads[i].retention == "" {
			failureLog(function, args, startTime, "", fmt.Sprintf("DELETE expected to succeed but got %v", err), err).Fatal()
			return
		}
	}

	successLogger(function, args, startTime).Info()
}

func testPutGetDeleteRetentionGovernanceMultipart() {
	functionName := "testPutGetDeleteRetentionGovernanceMultipart"
	testPutGetDeleteLockingRetentionMultipart(functionName, "GOVERNANCE")
}

// Test locking retention governance (multipart)
func testPutGetDeleteLockingRetentionMultipart(function, retentionMode string) {
	startTime := time.Now()
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "versioning-test-")
	object := "testObject"
	args := map[string]interface{}{
		"bucketName":    bucket,
		"objectName":    object,
		"retentionMode": retentionMode,
	}

	ctx := context.Background()

	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket:                     aws.String(bucket),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotImplemented: A header you provided implies functionality that is not implemented") {
			ignoreLog(function, args, startTime, "Versioning is not implemented").Info()
			return
		}
		failureLog(function, args, startTime, "", "CreateBucket failed", err).Fatal()
		return
	}

	fileSize := 30 * 1024 * 1024
	createTestObject(int64(fileSize), object)

	f, err := os.Open(object)
	if err != nil {
		failureLog(function, args, startTime, "", "Open testobject failed", err).Fatal()
		return
	}
	defer f.Close()
	defer os.Remove(object)

	defer cleanupBucket(bucket, function, args, startTime)

	oneMinuteRetention := time.Now().UTC().Add(time.Minute)
	twoMinutesRetention := oneMinuteRetention.Add(time.Minute)

	partSize := 5 * 1024 * 1024 // Set part size to 5 MB (minimum size for a part)

	// Upload version and save the version ID
	multipartUpload, err := s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:                    aws.String(bucket),
		Key:                       aws.String(object),
		ObjectLockMode:            types.ObjectLockMode(retentionMode),
		ObjectLockRetainUntilDate: aws.Time(oneMinuteRetention),
	})

	if err != nil {
		failureLog(function, args, startTime, "", "CreateMultipartupload API failed", err).Fatal()
		return
	}

	filePart := make([]byte, partSize)
	partCount := fileSize / partSize
	parts := make([]*string, partCount)
	for j := 0; j < partCount; j++ {
		_, err := f.ReadAt(filePart, int64(partSize*j))
		if err != nil {
			failureLog(function, args, startTime, "", "ReadAt failed", err).Fatal()
			return
		}
		r := bytes.NewReader(filePart)

		result, errUpload := s3Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(object),
			UploadId:   multipartUpload.UploadId,
			PartNumber: aws.Int32(int32(j + 1)),
			Body:       r,
		})
		if errUpload != nil {
			_, _ = s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      aws.String(object),
				UploadId: multipartUpload.UploadId,
			})
			failureLog(function, args, startTime, "", "UploadPart API failed for", errUpload).Fatal()
			return
		}
		parts[j] = result.ETag
	}

	completedParts := make([]types.CompletedPart, len(parts))
	for i, part := range parts {
		completedParts[i] = types.CompletedPart{
			ETag:       part,
			PartNumber: aws.Int32(int32(i + 1)),
		}
	}

	output, err := s3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts},
		UploadId: multipartUpload.UploadId,
	})
	if err != nil {
		failureLog(function, args, startTime, "", "CompleteMultipartUpload is expected to succeed but failed", errors.New("expected nil")).Fatal()
		return
	}

	versionId := *output.VersionId

	// Increase retention until date
	putRetentionInput := &s3.PutObjectRetentionInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: aws.String(versionId),
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionMode(retentionMode),
			RetainUntilDate: aws.Time(twoMinutesRetention),
		},
	}
	_, err = s3Client.PutObjectRetention(ctx, putRetentionInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("PutObjectRetention expected to succeed but got %v", err), err).Fatal()
		return
	}

	getRetentionInput := &s3.GetObjectRetentionInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: aws.String(versionId),
	}

	retentionOutput, err := s3Client.GetObjectRetention(ctx, getRetentionInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("GetObjectRetention expected to succeed but got %v", err), err).Fatal()
		return
	}

	// Compare until retention date with truncating precision less than second
	if retentionOutput.Retention.RetainUntilDate.Truncate(time.Second).String() != twoMinutesRetention.Truncate(time.Second).String() {
		failureLog(function, args, startTime, "", "Unexpected until retention date", nil).Fatal()
		return
	}

	// Lower retention until date, should fail
	putRetentionInput = &s3.PutObjectRetentionInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: aws.String(versionId),
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionMode(retentionMode),
			RetainUntilDate: aws.Time(oneMinuteRetention),
		},
	}
	_, err = s3Client.PutObjectRetention(ctx, putRetentionInput)
	if err == nil {
		failureLog(function, args, startTime, "", "PutObjectRetention expected to fail but succeeded", nil).Fatal()
		return
	}

	// Remove retention without governance bypass
	putRetentionInput = &s3.PutObjectRetentionInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: aws.String(versionId),
		Retention: &types.ObjectLockRetention{
			Mode: types.ObjectLockRetentionMode(""),
		},
	}

	_, err = s3Client.PutObjectRetention(ctx, putRetentionInput)
	if err == nil {
		failureLog(function, args, startTime, "", "Operation expected to fail but succeeded", nil).Fatal()
		return
	}

	if retentionMode == "GOVERNANCE" {
		// Remove governance retention without govenance bypass
		putRetentionInput = &s3.PutObjectRetentionInput{
			Bucket:                    aws.String(bucket),
			Key:                       aws.String(object),
			VersionId:                 aws.String(versionId),
			BypassGovernanceRetention: aws.Bool(true),
			Retention: &types.ObjectLockRetention{
				Mode: types.ObjectLockRetentionMode(""),
			},
		}

		_, err = s3Client.PutObjectRetention(ctx, putRetentionInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("Expected to succeed but failed with %v", err), err).Fatal()
			return
		}
	}

	successLogger(function, args, startTime).Info()
}