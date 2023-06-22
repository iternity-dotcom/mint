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

// Tests ilm deletion rules
func testExpiry() {
	lConfigFuture := &s3.BucketLifecycleConfiguration{
		Rules: []*s3.LifecycleRule{
			{
				ID:     aws.String("expirydateinfuture"),
				Status: aws.String("Enabled"),
				Expiration: &s3.LifecycleExpiration{
					Date: aws.Time(time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, 1)),
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

	lConfigPastPrefix := &s3.BucketLifecycleConfiguration{
		Rules: []*s3.LifecycleRule{
			{
				ID:     aws.String("expirydateinpast"),
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
		// testExpiry case - 1.
		// Expire date in future, object not deleted
		{
			lConfig:     lConfigFuture,
			object:      "object",
			expDeletion: false,
		},
		// testExpiry case - 2.
		// Expire date in past, rule without prefix filter
		{
			lConfig:     lConfigPast,
			object:      "object",
			expDeletion: true,
		},
		// testExpiry case - 3.
		// Expire date in past, rule with prefix filter does not match
		{
			lConfig:     lConfigPastPrefix,
			object:      "object",
			expDeletion: false,
		},
		// testExpiry case - 4.
		// Expire date in past, rule with prefix filter matches
		{
			lConfig:     lConfigPastPrefix,
			object:      "prefix/object",
			expDeletion: true,
		},
	}

	for i, testCase := range testCases {
		execTestExpiry(i, testCase)
	}

}

func execTestExpiry(i int, testCase struct {
	lConfig     *s3.BucketLifecycleConfiguration
	object      string
	expDeletion bool
}) {
	// initialize logging params
	startTime := time.Now()
	function := "testExpiry"
	bucketName := uniqueBucketName()
	args := map[string]interface{}{
		"testCase":    i,
		"bucketName":  bucketName,
		"objectName":  testCase.object,
		"expDeletion": testCase.expDeletion,
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

	result, err := s3Client.GetObject(getInput)
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
