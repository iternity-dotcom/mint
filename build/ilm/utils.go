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
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyz01234569"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

// different kinds of test failures
const (
	PASS = "PASS" // Indicate that a test passed
	FAIL = "FAIL" // Indicate that a test failed
)

// Create a testfile
func createTestfile(size int64, name string) {
	f, err := os.Create(name)
	if err != nil {
		log.Fatal("Failed to create testfile")
	}
	if err := f.Truncate(size); err != nil {
		log.Fatal("Failed to truncate")
	}
}

type mintJSONFormatter struct {
}

func (f *mintJSONFormatter) Format(entry *log.Entry) ([]byte, error) {
	data := make(log.Fields, len(entry.Data))
	for k, v := range entry.Data {
		switch v := v.(type) {
		case error:
			// Otherwise errors are ignored by `encoding/json`
			// https://github.com/sirupsen/logrus/issues/137
			data[k] = v.Error()
		default:
			data[k] = v
		}
	}

	serialized, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal fields to JSON, %w", err)
	}
	return append(serialized, '\n'), nil
}

// log successful test runs
func successLogger(function string, args map[string]interface{}, startTime time.Time) *log.Entry {
	// calculate the test case duration
	duration := time.Since(startTime)
	// log with the fields as per mint
	fields := log.Fields{"name": "ilm", "function": function, "args": args, "duration": duration.Nanoseconds() / 1000000, "status": PASS}
	return log.WithFields(fields)
}

// log not applicable test runs
func ignoreLog(function string, args map[string]interface{}, startTime time.Time, alert string) *log.Entry {
	// calculate the test case duration
	duration := time.Since(startTime)
	// log with the fields as per mint
	fields := log.Fields{"name": "ilm", "function": function, "args": args,
		"duration": duration.Nanoseconds() / 1000000, "status": "NA", "alert": strings.Split(alert, " ")[0] + " is NotImplemented"}
	return log.WithFields(fields)
}

// log failed test runs
func failureLog(function string, args map[string]interface{}, startTime time.Time, alert string, message string, err error) *log.Entry {
	// calculate the test case duration
	duration := time.Since(startTime)
	var fields log.Fields
	// log with the fields as per mint
	if err != nil {
		fields = log.Fields{"name": "ilm", "function": function, "args": args,
			"duration": duration.Nanoseconds() / 1000000, "status": FAIL, "alert": alert, "message": message, "error": err}
	} else {
		fields = log.Fields{"name": "ilm", "function": function, "args": args,
			"duration": duration.Nanoseconds() / 1000000, "status": FAIL, "alert": alert, "message": message}
	}
	return log.WithFields(fields)
}

func randString(n int, src rand.Source, prefix string) string {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return prefix + string(b[0:30-len(prefix)])
}

func createS3Client(envCfg envConfig) (*s3.S3, error) {
	creds := credentials.NewStaticCredentials(envCfg.accessKey, envCfg.secretKey, "")
	s3Config := &aws.Config{
		Credentials:      creds,
		Endpoint:         aws.String(envCfg.sdkEndpoint),
		Region:           aws.String("us-east-1"),
		S3ForcePathStyle: aws.Bool(true),
	}
	newSession, err := session.NewSession(s3Config)

	// Create an S3 service object in the default region.
	return s3.New(newSession, s3Config), err
}

type envConfig struct {
	endpoint       string
	sdkEndpoint    string
	accessKey      string
	secretKey      string
	secure         bool
	remoteTierName string
}

func loadEnvConfig() envConfig {
	endpoint := os.Getenv("SERVER_ENDPOINT")
	accessKey := os.Getenv("ACCESS_KEY")
	secretKey := os.Getenv("SECRET_KEY")
	secureVal := os.Getenv("ENABLE_HTTPS")
	sdkEndpoint := "http://" + endpoint
	if secureVal == "1" {
		sdkEndpoint = "https://" + endpoint
	}
	remoteTierName := os.Getenv("REMOTE_TIER_NAME")

	return envConfig{
		endpoint:       endpoint,
		accessKey:      accessKey,
		secretKey:      secretKey,
		secure:         secureVal == "1",
		sdkEndpoint:    sdkEndpoint,
		remoteTierName: remoteTierName,
	}
}

func getMaxScannerWaitSeconds() int {
	maxWaitS := os.Getenv("MAX_SCANNER_WAIT_SECONDS")
	if maxWaitS != "" {
		if i, err := strconv.Atoi(maxWaitS); err == nil {
			return i
		}
	}

	return 0
}

var randSrc = rand.NewSource(time.Now().UnixNano())
var randMu sync.Mutex

func uniqueBucketName() string {
	randMu.Lock()
	defer randMu.Unlock()

	return randString(60, randSrc, "ilm-test-")
}
