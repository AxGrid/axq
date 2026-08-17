package utils

import (
	"fmt"
	"strings"

	"github.com/axgrid/axq/domain"
)

const (
	// DefaultBucketNamespace — первая часть имени бакета, если её не задали явно.
	DefaultBucketNamespace = "axq"

	bucketNameMinLen = 6
	bucketNameMaxLen = 50
)

// ResolveBucketName возвращает имя бакета для очереди. Явно заданное имя
// побеждает, иначе оно собирается из namespace, стенда и имени очереди —
// например stellar-bets-prod-events.
//
// Функция обязана давать одинаковый ответ у архивера и у b2-ридера: ридер
// строит URL файла из имени бакета, не заглядывая никуда, кроме опций.
func ResolveBucketName(b2 domain.B2Options, queue string) (string, error) {
	if b2.Bucket != "" {
		if err := ValidateBucketName(b2.Bucket); err != nil {
			return "", err
		}
		return b2.Bucket, nil
	}
	namespace := b2.Namespace
	if namespace == "" {
		namespace = DefaultBucketNamespace
	}
	return BucketName(namespace, b2.Stand, queue)
}

// BucketName склеивает имя бакета из частей, приводя их к алфавиту B2. Пустые
// части выкидываются, поэтому незаданный стенд не даёт двойного дефиса.
func BucketName(parts ...string) (string, error) {
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = sanitizeBucketPart(part); part != "" {
			cleaned = append(cleaned, part)
		}
	}
	name := strings.Join(cleaned, "-")
	if err := ValidateBucketName(name); err != nil {
		return "", fmt.Errorf("%w (parts: %s)", err, strings.Join(parts, ", "))
	}
	return name, nil
}

// ValidateBucketName проверяет имя по требованиям B2: 6–50 символов из букв,
// цифр и дефиса, не начинается с зарезервированного b2-. Ошибку лучше получить
// на старте сервиса, чем ответом API на первой заливке.
func ValidateBucketName(name string) error {
	if len(name) < bucketNameMinLen {
		return fmt.Errorf("invalid b2 bucket name %q: shorter than %d characters", name, bucketNameMinLen)
	}
	if len(name) > bucketNameMaxLen {
		return fmt.Errorf("invalid b2 bucket name %q: longer than %d characters", name, bucketNameMaxLen)
	}
	if strings.HasPrefix(name, "b2-") {
		return fmt.Errorf("invalid b2 bucket name %q: prefix b2- is reserved by backblaze", name)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if isBucketNameChar(c) {
			continue
		}
		if c == '-' && i != 0 && i != len(name)-1 {
			continue
		}
		return fmt.Errorf("invalid b2 bucket name %q: unexpected character %q at position %d", name, string(c), i)
	}
	return nil
}

// sanitizeBucketPart приводит часть имени к [a-z0-9-]: остальное схлопывается в
// один дефис, чтобы имя очереди вроде my_queue не ломало бакет.
func sanitizeBucketPart(part string) string {
	var sb strings.Builder
	sb.Grow(len(part))
	dash := false
	for i := 0; i < len(part); i++ {
		c := part[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if isBucketNameChar(c) {
			sb.WriteByte(c)
			dash = false
			continue
		}
		if !dash && sb.Len() > 0 {
			sb.WriteByte('-')
			dash = true
		}
	}
	return strings.Trim(sb.String(), "-")
}

func isBucketNameChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
}
