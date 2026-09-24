package models_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nyaruka/courier/v26/core/models"
	"github.com/nyaruka/courier/v26/runtime"
	"github.com/nyaruka/courier/v26/test"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Without S3, attachments are written to the local attachments directory and served from the media domain. These
// tests need no backing services - just a runtime with no S3 service and a temporary directory.
func TestSaveAttachmentLocally(t *testing.T) {
	defer uuids.SetGenerator(uuids.DefaultGenerator)
	uuids.SetGenerator(uuids.NewSeededGenerator(1234, time.Now))

	cfg := runtime.NewDefaultConfig()
	cfg.MediaDomain = "example.com"
	cfg.AttachmentsDir = t.TempDir()
	cfg.AttachmentsURLPath = "/rp/media"
	rt := &runtime.Runtime{Config: cfg} // S3 deliberately nil

	ch := test.NewMockChannel("8eb23e93-5ecb-45ba-b726-3b064e0c56ab", "WZ", "12065551212", "US", []string{urns.WhatsApp.Prefix}, nil)

	storageURL, err := models.SaveAttachment(context.Background(), rt, ch, "image/jpeg", []byte("not really a jpeg"), "jpg")
	require.NoError(t, err)
	assert.Equal(t, "https://example.com/rp/media/attachments/1/15a2ee5e-5e45-4711-8e0f-6b2abe4360d8.jpg", storageURL)

	path := filepath.Join(cfg.AttachmentsDir, "attachments", "1", "15a2ee5e-5e45-4711-8e0f-6b2abe4360d8.jpg")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "not really a jpeg", string(data))

	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o644), info.Mode().Perm())

	// nothing but the file is left behind
	entries, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)
	assert.Len(t, entries, 1)

	// an extension that isn't safe for a filename is dropped, and the URL path has a default
	cfg.AttachmentsURLPath = "/media"
	storageURL, err = models.SaveAttachment(context.Background(), rt, ch, "application/octet-stream", []byte("data"), "../etc")
	require.NoError(t, err)
	assert.Equal(t, "https://example.com/media/attachments/1/f8844b62-b014-4975-9a98-cfcce3019710", storageURL)
	_, err = os.Stat(filepath.Join(cfg.AttachmentsDir, "attachments", "1", "f8844b62-b014-4975-9a98-cfcce3019710"))
	assert.NoError(t, err)
}

func TestSaveAttachmentWithoutStorage(t *testing.T) {
	rt := &runtime.Runtime{Config: runtime.NewDefaultConfig()} // no S3 and no attachments directory
	ch := test.NewMockChannel("8eb23e93-5ecb-45ba-b726-3b064e0c56ab", "WZ", "12065551212", "US", []string{urns.WhatsApp.Prefix}, nil)

	_, err := models.SaveAttachment(context.Background(), rt, ch, "image/jpeg", []byte("data"), "jpg")
	assert.ErrorIs(t, err, models.ErrNoAttachmentStorage)
}
