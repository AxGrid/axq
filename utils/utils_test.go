package utils

import (
	"github.com/stretchr/testify/assert"
	"gopkg.in/kothar/go-backblaze.v0"
	"testing"
)

func TestAuthorizeB2(t *testing.T) {
	c := backblaze.Credentials{
		KeyID:          b2KeyId,
		ApplicationKey: b2AppKey,
	}

	token, err := AuthorizeB2(c)
	assert.Nil(t, err)
	assert.NotEqual(t, "", token)
}

func TestDownloadFileByName(t *testing.T) {
	c := backblaze.Credentials{
		KeyID:          b2KeyId,
		ApplicationKey: b2AppKey,
	}

	token, err := AuthorizeB2(c)
	assert.Nil(t, err)

	content, err := DownloadFileByName("", b2Endpoint, token)
	assert.Nil(t, err)
	assert.NotEmpty(t, content)
}
