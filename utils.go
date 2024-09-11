package syncer

import "strings"

func EndWith(haystack string, suffix ...string) (bool, string) {

	for _, s := range suffix {
		if strings.HasSuffix(haystack, s) {
			return true, s
		}
	}

	return false, ""
}
