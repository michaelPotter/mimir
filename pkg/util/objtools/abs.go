// SPDX-License-Identifier: AGPL-3.0-only

package objtools

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/grafana/dskit/backoff"
	"github.com/pkg/errors"
)

type azureBucket struct {
	azblob.Client
	containerClient         container.Client
	containerName           string
	copyStatusBackoffConfig backoff.Config
}

type AzureClientConfig struct {
	ContainerURL      string
	AccountName       string
	AccountKey        string
	CopyStatusBackoff backoff.Config
}

func (c *AzureClientConfig) RegisterFlags(prefix string, f *flag.FlagSet) {
	f.StringVar(&c.ContainerURL, prefix+"container-url", "", "The container URL for Azure Blob Storage.")
	f.StringVar(&c.AccountName, prefix+"account-name", "", "The storage account name for Azure Blob Storage.")
	f.StringVar(&c.AccountKey, prefix+"account-key", "", "The storage account key for Azure Blob Storage.")
	f.DurationVar(&c.CopyStatusBackoff.MinBackoff, prefix+"copy-status-backoff-min-duration", 15*time.Second, "The minimum amount of time to back off per copy operation sourced from this bucket.")
	f.DurationVar(&c.CopyStatusBackoff.MaxBackoff, prefix+"copy-status-backoff-max-duration", 20*time.Second, "The maximum amount of time to back off per copy operation sourced from this bucket.")
	f.IntVar(&c.CopyStatusBackoff.MaxRetries, prefix+"copy-status-backoff-max-retries", 40, "The maximum number of retries while checking the copy status of copies sourced from this bucket.")
}

func (c *AzureClientConfig) Validate(prefix string) error {
	if c.AccountName == "" {
		return fmt.Errorf("the Azure bucket's account name (%s) is required", prefix+"account-name")
	}
	if c.AccountKey == "" {
		return fmt.Errorf("the Azure bucket's account key (%s) is required", prefix+"account-key")
	}
	return nil
}

func (c *AzureClientConfig) ToBucket() (Bucket, error) {
	urlParts, err := blob.ParseURL(c.ContainerURL)
	if err != nil {
		return nil, err
	}
	containerName := urlParts.ContainerName
	if containerName == "" {
		return nil, errors.New("container name missing from Azure bucket URL")
	}
	serviceURL, found := strings.CutSuffix(c.ContainerURL, containerName)
	if !found {
		return nil, errors.New("malformed or unexpected Azure bucket URL")
	}
	keyCred, err := azblob.NewSharedKeyCredential(c.AccountName, c.AccountKey)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get Azure shared key credential")
	}
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, keyCred, nil)
	if err != nil {
		return nil, err
	}
	containerClient, err := container.NewClientWithSharedKeyCredential(c.ContainerURL, keyCred, nil)
	if err != nil {
		return nil, err
	}
	return &azureBucket{
		Client:                  *client,
		containerClient:         *containerClient,
		containerName:           containerName,
		copyStatusBackoffConfig: c.CopyStatusBackoff,
	}, nil
}

func (bkt *azureBucket) Get(ctx context.Context, objectName string) (io.ReadCloser, error) {
	client := bkt.containerClient.NewBlobClient(objectName)
	response, err := client.DownloadStream(ctx, nil)
	if err != nil {
		return nil, err
	}
	return response.Body, nil
}

func (bkt *azureBucket) ServerSideCopy(ctx context.Context, objectName string, dstBucket Bucket) error {
	sourceClient := bkt.containerClient.NewBlobClient(objectName)
	d, ok := dstBucket.(*azureBucket)
	if !ok {
		return errors.New("destination bucket wasn't an Azure bucket")
	}
	dstClient := d.containerClient.NewBlobClient(objectName)

	start := time.Now()
	expiry := start.Add(10 * time.Minute)

	sasURL, err := sourceClient.GetSASURL(sas.BlobPermissions{Read: true}, expiry, &blob.GetSASURLOptions{StartTime: &start})
	if err != nil {
		return err
	}

	var copyStatus *blob.CopyStatusType
	var copyStatusDescription *string

	response, err := dstClient.StartCopyFromURL(ctx, sasURL, nil)
	if err != nil {
		if !bloberror.HasCode(err, bloberror.PendingCopyOperation) {
			return err
		}
		// There's already a copy operation. Assume it was initiated by us and a restart occurred, so check for the copy status.
		copyStatus, copyStatusDescription, err = checkCopyStatus(ctx, dstClient)
		if err != nil {
			return err
		}
	} else {
		// Note: no copy status description is currently provided from StartCopyFromURL
		// see https://learn.microsoft.com/en-us/rest/api/storageservices/copy-blob
		copyStatus = response.CopyStatus
	}

	backoff := backoff.New(ctx, d.copyStatusBackoffConfig)
	for {
		if copyStatus == nil {
			return errors.New("no copy status present for blob copy")
		}

		switch *copyStatus {
		case blob.CopyStatusTypeSuccess:
			return nil
		case blob.CopyStatusTypeFailed:
			if copyStatusDescription != nil {
				return errors.Errorf("copy failed, description: %s", *copyStatusDescription)
			}
			return errors.New("copy failed")
		case blob.CopyStatusTypeAborted:
			return errors.New("copy aborted")
		case blob.CopyStatusTypePending:
			// proceed
		default:
			return errors.Errorf("unrecognized copy status: %v", *copyStatus)
		}

		if !backoff.Ongoing() {
			break
		}
		backoff.Wait()

		copyStatus, copyStatusDescription, err = checkCopyStatus(ctx, dstClient)
		if err != nil {
			return err
		}
	}

	return errors.Wrap(backoff.Err(), "waiting for blob copy status")
}

func checkCopyStatus(ctx context.Context, client *blob.Client) (*blob.CopyStatusType, *string, error) {
	response, err := client.GetProperties(ctx, nil)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed while checking copy status")
	}
	return response.CopyStatus, response.CopyStatusDescription, nil
}

func (bkt *azureBucket) ClientSideCopy(ctx context.Context, objectName string, dstBucket Bucket) error {
	sourceClient := bkt.containerClient.NewBlobClient(objectName)
	response, err := sourceClient.DownloadStream(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed while getting source object from Azure")
	}
	if response.ContentLength == nil {
		return errors.New("source object from Azure did not contain a content length")
	}
	body := response.DownloadResponse.Body
	if err := dstBucket.Upload(ctx, objectName, body, *response.ContentLength); err != nil {
		_ = body.Close()
		return errors.New("failed uploading source object from Azure to destination")
	}
	return errors.Wrap(body.Close(), "failed closing Azure source object reader")
}

func (bkt *azureBucket) ListPrefix(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	if prefix != "" && !strings.HasSuffix(prefix, Delim) {
		prefix = prefix + Delim
	}

	list := make([]string, 0, 10)
	if recursive {
		pager := bkt.containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: &prefix})
		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				return nil, err
			}
			for _, blobItem := range page.Segment.BlobItems {
				list = append(list, *blobItem.Name)
			}
		}
	} else {
		pager := bkt.containerClient.NewListBlobsHierarchyPager(Delim, &container.ListBlobsHierarchyOptions{Prefix: &prefix})
		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				return nil, err
			}
			for _, blobItem := range page.Segment.BlobItems {
				list = append(list, *blobItem.Name)
			}
			for _, blobPrefix := range page.Segment.BlobPrefixes {
				list = append(list, *blobPrefix.Name)
			}
		}
	}

	var hasPrefix bool
	for i, s := range list {
		list[i], hasPrefix = strings.CutPrefix(s, prefix)
		if !hasPrefix {
			return nil, errors.Errorf("listPrefix: path has invalid prefix: %v, expected prefix: %v", s, prefix)
		}
	}

	return list, nil
}

func (bkt *azureBucket) Upload(ctx context.Context, objectName string, reader io.Reader, _ int64) error {
	_, err := bkt.UploadStream(ctx, bkt.containerName, objectName, reader, nil)
	return err
}

func (bkt *azureBucket) Name() string {
	return bkt.containerName
}
