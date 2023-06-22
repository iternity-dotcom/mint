# Lifecycle Management tests

The `ilm` folder contains all tests related to the lifecycle management feature. These tests are executed for every minio instance that is able to store a lifecycle bucket configuration. Tests with versioned buckets are only executed if the minio instance supports the creation of versioned buckets.

Some of the tests rely on the data scanner and therefore need to wait until it has run the next time. You can control the maximum time to wait for the scanner, but it is also helpful to reduce the scanner interval on the minio instance unter test. 

## Testing transition and restore

Testing transition and restore of objects requires the instance under test to have a remote tier configured. [See: mc admin tier](https://docs.min.io/minio/baremetal/reference/minio-mc-admin/mc-admin-tier.html#mc-admin-tier-add)

To activate testing transition and restore the tests have to be started with environment varianle `REMOTE_TIER_NAME` set to the `TIER_NAME` configured in the minio instance under test.

Example:

```shell
mc admin tier add s3 mynasxl MY_REMOTE --endpoint http://127.0.0.1:9999 --bucket remotebucket --access-key accesskey --secret-key secretkey --storage-class STANDARD
```

Adds the minio instance running on 127.0.0.1:9999 as tier with `TIER_NAME` set to `MY_REMOTE`.
Thus the mint ilm tests need to be started with `REMOTE_TIER_NAME=MY_REMOTE` if you want to activate transition and restore testing.

## Configurations for ilm tests

| Environment variable           | Description                                                                                                                                      |
|:-------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------|
| `REMOTE_TIER_NAME` | Set this to the name of the tier configured on the minio instance under test to activate the transition and restore tests. Example: `MY_REMOTE`. |
| `MAX_SCANNER_WAIT_SECONDS`          | Defines maximum time to wait for the data scanner in tests that rely on the scanner. Default value is `120`.                                     |
