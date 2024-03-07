package utils

import "os"

var (
	b2KeyId    = os.Getenv("AX_TEST_B2_KEY_ID")
	b2AppKey   = os.Getenv("AX_TEST_B2_APP_KEY")
	b2Endpoint = getenv("AX_TEST_B2_ENDPOINT", "s3.us-east-005.backblazeb2.com")
)

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
