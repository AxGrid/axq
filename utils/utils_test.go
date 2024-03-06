package utils

import (
	"github.com/stretchr/testify/assert"
	"gopkg.in/kothar/go-backblaze.v0"
	"testing"
)

func TestAuthorizeB2(t *testing.T) {
	c := backblaze.Credentials{
		KeyID:          "39e21a181c18",
		ApplicationKey: "0054ca0ddb7e31d7c935a5df8f49c55514ca8372f8",
	}

	token, err := AuthorizeB2(c)
	assert.Nil(t, err)
	assert.NotEqual(t, "", token)
}

func TestDownloadFileByName(t *testing.T) {
	c := backblaze.Credentials{
		KeyID:          "39e21a181c18",
		ApplicationKey: "0054ca0ddb7e31d7c935a5df8f49c55514ca8372f8",
	}

	token, err := AuthorizeB2(c)
	assert.Nil(t, err)

	content, err := DownloadFileByName("axqueue-098f6bcd4621d373cade4e832627b4f6/1", token)
	assert.Nil(t, err)
	assert.NotEmpty(t, content)
}
