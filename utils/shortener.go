package utils

import (
	"fmt"
	"github.com/google/uuid"
	"strings"
)

func Shortener(name string) string {
	splitName := strings.Split(name, "_")
	if len(splitName) == 2 {
		if _, err := uuid.Parse(splitName[1]); err != nil {
			return name
		}
		name = fmt.Sprintf("%s_%s...%s", splitName[0], splitName[1][:4], splitName[1][31:len(splitName[1])-1])
	}
	return name
}
