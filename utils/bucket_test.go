package utils

import (
	"testing"

	"github.com/axgrid/axq/domain"
	"github.com/stretchr/testify/assert"
)

func TestResolveBucketName(t *testing.T) {
	t.Run("namespace stand queue", func(t *testing.T) {
		name, err := ResolveBucketName(domain.B2Options{Namespace: "stellar-bets", Stand: "prod"}, "events")
		assert.NoError(t, err)
		assert.Equal(t, "stellar-bets-prod-events", name)
	})

	t.Run("explicit bucket wins", func(t *testing.T) {
		name, err := ResolveBucketName(domain.B2Options{Bucket: "legacy-bucket-name", Namespace: "stellar-bets", Stand: "prod"}, "events")
		assert.NoError(t, err)
		assert.Equal(t, "legacy-bucket-name", name)
	})

	t.Run("default namespace", func(t *testing.T) {
		name, err := ResolveBucketName(domain.B2Options{Stand: "prod"}, "events")
		assert.NoError(t, err)
		assert.Equal(t, "axq-prod-events", name)
	})

	t.Run("empty stand collapses", func(t *testing.T) {
		name, err := ResolveBucketName(domain.B2Options{Namespace: "stellar-bets"}, "events")
		assert.NoError(t, err)
		assert.Equal(t, "stellar-bets-events", name)
	})

	// Обе стороны обязаны получить одно имя: ридер строит URL файла из него и
	// больше ниоткуда.
	t.Run("archiver and reader agree", func(t *testing.T) {
		b2 := domain.B2Options{Namespace: "stellar-bets", Stand: "prod"}
		archiver, err := ResolveBucketName(b2, "events")
		assert.NoError(t, err)
		reader, err := ResolveBucketName(b2, "events")
		assert.NoError(t, err)
		assert.Equal(t, archiver, reader)
	})
}

func TestBucketNameSanitize(t *testing.T) {
	cases := map[string]string{
		"Stellar_Bets": "stellar-bets",
		"prod":         "prod",
		"my_queue.v2":  "my-queue-v2",
		"--events--":   "events",
	}
	for in, want := range cases {
		assert.Equal(t, want, sanitizeBucketPart(in), in)
	}

	name, err := BucketName("Stellar_Bets", "PROD", "my_queue.v2")
	assert.NoError(t, err)
	assert.Equal(t, "stellar-bets-prod-my-queue-v2", name)
}

func TestValidateBucketName(t *testing.T) {
	assert.NoError(t, ValidateBucketName("stellar-bets-prod-events"))
	assert.NoError(t, ValidateBucketName("axq123"))

	assert.Error(t, ValidateBucketName("short"))
	assert.Error(t, ValidateBucketName("b2-reserved-name"))
	assert.Error(t, ValidateBucketName("-leading-dash"))
	assert.Error(t, ValidateBucketName("trailing-dash-"))
	assert.Error(t, ValidateBucketName("under_score"))
	assert.Error(t, ValidateBucketName("события-очереди"))
	// 51 символ — на символ длиннее лимита B2
	assert.Error(t, ValidateBucketName("aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeef"))
}

func TestBucketNameTooShort(t *testing.T) {
	_, err := BucketName("", "", "ev")
	assert.Error(t, err)
}
