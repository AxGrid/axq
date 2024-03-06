package utils

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/axgrid/axq/domain"
	"github.com/google/uuid"
	"github.com/speps/go-hashids"
	"gopkg.in/kothar/go-backblaze.v0"
	"io"
	"net/http"
)

type B2FileInfo struct {
	ContentMD5 string `json:"contentMd5"`
}

type B2Authorize struct {
	Token string `json:"authorizationToken"`
}

func AuthorizeB2(creds backblaze.Credentials) (string, error) {
	req, err := http.NewRequest("GET", "https://api.backblazeb2.com/b2api/v2/b2_authorize_account", nil)
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(creds.KeyID, creds.ApplicationKey)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	var b2auth B2Authorize
	if err := json.NewDecoder(res.Body).Decode(&b2auth); err != nil {
		return "", err
	}

	return b2auth.Token, nil
}

func GetFileInfoById(fileId, credentials string) (B2FileInfo, error) {
	req, err := http.NewRequest(http.MethodGet, "https://api005.backblazeb2.com/b2api/v2/b2_get_file_info?fileId="+fileId, nil)
	if err != nil {
		return B2FileInfo{}, nil
	}
	req.Header.Add("Authorization", credentials)
	req.Header.Add("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return B2FileInfo{}, err
	}
	defer res.Body.Close()

	var body B2FileInfo
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return B2FileInfo{}, err
	}

	return body, nil
}

func DownloadFileById(fileId, credentials string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, "https://api.backblazeb2.com/b2api/v2/b2_download_file_by_id?fileId="+fileId, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", credentials)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func DownloadFileByName(bucket, endpoint, filename string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("https://%s.%s/%s", bucket, endpoint, filename), nil)
	if err != nil {
		return nil, err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return nil, domain.ErrB2FileNotFound
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func GetFileMeta(bucket, endpoint, filename string) (string, error) {
	req, err := http.NewRequest(http.MethodHead, fmt.Sprintf("https://%s.%s/%s", bucket, endpoint, filename), nil)
	if err != nil {
		return "", err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", nil
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return "", domain.ErrB2FileNotFound
	}

	return res.Header.Get("hash"), nil
}

func GetMD5Hash(content []byte, salt string) string {
	hash := md5.New()
	hash.Write(content)
	return fmt.Sprintf("%x", hash.Sum([]byte(salt)))
}

func GetBlobFileName(hashId *hashids.HashID, name string, fid uint64) (string, error) {
	if hashId != nil {
		hexFileName, err := hashId.EncodeInt64([]int64{int64(fid)})
		if err != nil {
			return "", err
		}
		fName := fmt.Sprintf("%s/%s.axq", name, hexFileName)
		return fName, nil
	}
	return fmt.Sprintf("%s/%d.axq", name, fid), nil
}

type Numered interface {
	GetId() uint64
}

type GenericChan[T []Numered] chan T

type SortParams struct {
	Ctx context.Context
	Rcv <-chan []Numered
}

func UUIDString() string {
	return uuid.NewString()
}
