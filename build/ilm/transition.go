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
	"github.com/aws/aws-sdk-go/aws/awserr"
	"io/ioutil"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Tests ilm transition rules
func testTransition() {
	lConfigFuture := &s3.BucketLifecycleConfiguration{
		Rules: []*s3.LifecycleRule{
			{
				ID:     aws.String("transitiondateinfuture"),
				Status: aws.String("Enabled"),
				Transitions: []*s3.Transition{
					{
						Date:         aws.Time(time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, 1)),
						StorageClass: aws.String(tierName),
					},
				},
				Filter: &s3.LifecycleRuleFilter{
					Prefix: aws.String(""),
				},
			},
		},
	}

	lConfigPast := &s3.BucketLifecycleConfiguration{
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

	lConfigPastPrefix := &s3.BucketLifecycleConfiguration{
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
					Prefix: aws.String("prefix"),
				},
			},
		},
	}

	testCases := []struct {
		lConfig       *s3.BucketLifecycleConfiguration
		object        string
		expTransition bool
	}{
		// testTransition case - 1.
		// Transition date in future, object not transitioned
		{
			lConfig:       lConfigFuture,
			object:        "object",
			expTransition: false,
		},
		// testTransition case - 2.
		// Transition date in past, rule without prefix filter
		{
			lConfig:       lConfigPast,
			object:        "object",
			expTransition: true,
		},
		// testTransition case - 3.
		// Transition date in past, rule with prefix filter does not match
		{
			lConfig:       lConfigPastPrefix,
			object:        "object",
			expTransition: false,
		},
		// testTransition case - 4.
		// Transition date in past, rule with prefix filter matches
		{
			lConfig:       lConfigPastPrefix,
			object:        "prefix/object",
			expTransition: true,
		},
	}

	for i, testCase := range testCases {
		execTestTransition(i, testCase)
	}

}

func execTestTransition(i int, testCase struct {
	lConfig       *s3.BucketLifecycleConfiguration
	object        string
	expTransition bool
}) {
	// initialize logging params
	startTime := time.Now()
	function := "testTransition"
	bucketName := uniqueBucketName()
	args := map[string]interface{}{
		"testCase":      i,
		"bucketName":    bucketName,
		"objectName":    testCase.object,
		"expTransition": testCase.expTransition,
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
		LifecycleConfiguration: testCase.lConfig,
	})
	if err != nil {
		failureLog(function, args, startTime, "", "Put LifecycleConfiguration failed", err).Error()
		return
	}

	putInput1 := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader("my content 1")),
		Bucket: aws.String(bucketName),
		Key:    aws.String(testCase.object),
	}
	_, err = s3Client.PutObject(putInput1)
	if err != nil {
		failureLog(function, args, startTime, "", "PUT expected to succeed but failed", err).Error()
		return
	}

	getInput := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(testCase.object),
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

		if testCase.expTransition && result.StorageClass != nil && *(result.StorageClass) == tierName {
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	if testCase.expTransition && (result.StorageClass == nil || *result.StorageClass != tierName) {
		failureLog(function, args, startTime, "", "Expected object to be transitioned.", nil).Error()
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

	successLogger(function, args, startTime).Info()
}

// Tests ilm expiry on transitioned objects
func testExpireTransitioned() {
	lConfigExpiry := &s3.BucketLifecycleConfiguration{
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

	lConfigExpiryPrefix := &s3.BucketLifecycleConfiguration{
		Rules: []*s3.LifecycleRule{
			{
				ID:     aws.String("expirydateinpastprefix"),
				Status: aws.String("Enabled"),
				Expiration: &s3.LifecycleExpiration{
					Date: aws.Time(time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -2)),
				},
				Filter: &s3.LifecycleRuleFilter{
					Prefix: aws.String("prefix"),
				},
			},
		},
	}

	testCases := []struct {
		lConfig     *s3.BucketLifecycleConfiguration
		object      string
		expDeletion bool
	}{
		// testExpireTransitioned case - 1.
		// Expire date in past, rule without prefix filter
		{
			lConfig:     lConfigExpiry,
			object:      "object",
			expDeletion: true,
		},
		// testExpireTransitioned case - 3.
		// Expire date in past, rule with prefix filter does not match
		{
			lConfig:     lConfigExpiryPrefix,
			object:      "object",
			expDeletion: false,
		},
		// testExpireTransitioned case - 3.
		// Expire date in past, rule with prefix filter matches
		{
			lConfig:     lConfigExpiryPrefix,
			object:      "prefix/object",
			expDeletion: true,
		},
	}

	for i, testCase := range testCases {
		execTestExpireTransitioned(i, testCase)
	}
}

func execTestExpireTransitioned(i int, testCase struct {
	lConfig     *s3.BucketLifecycleConfiguration
	object      string
	expDeletion bool
}) {
	// initialize logging params
	startTime := time.Now()
	function := "testExpireTransitioned"
	bucketName := uniqueBucketName()
	args := map[string]interface{}{
		"testCase":    i,
		"bucketName":  bucketName,
		"objectName":  testCase.object,
		"expDeletion": testCase.expDeletion,
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

	putInput1 := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader("my content 1")),
		Bucket: aws.String(bucketName),
		Key:    aws.String(testCase.object),
	}
	_, err = s3Client.PutObject(putInput1)
	if err != nil {
		failureLog(function, args, startTime, "", "PUT expected to succeed but failed", err).Error()
		return
	}

	getInput := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(testCase.object),
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

	_, err = s3Client.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket:                 aws.String(bucketName),
		LifecycleConfiguration: testCase.lConfig,
	})
	if err != nil {
		failureLog(function, args, startTime, "", "Put LifecycleConfiguration for expiry failed", err).Error()
		return
	}

	getInputAfterNewLc := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(testCase.object),
	}

	_, err = s3Client.GetObject(getInputAfterNewLc)
	if err != nil {
		aerr, ok := err.(awserr.Error)
		if !ok {
			failureLog(function, args, startTime, "", "Unexpected non aws error on GetObject", err).Error()
			return
		}
		if testCase.expDeletion && aerr.Code() == "NotFound" {
			successLogger(function, args, startTime).Info()
			return
		}

		failureLog(function, args, startTime, "", "Unexpected aws error on GetObject", err).Error()
		return
	}

	if testCase.expDeletion {
		failureLog(function, args, startTime, "", "Expected object to be deleted", nil).Error()
		return
	}

	successLogger(function, args, startTime).Info()
}
