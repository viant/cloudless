package processor

import (
	"fmt"
	"github.com/google/uuid"
	"strings"
	"time"
)

func expandURL(URL string, time time.Time) string {
	if count := strings.Count(URL, uuidVar); count > 0 {
		URL = strings.Replace(URL, uuidVar, uuid.New().String(), count)
	}
	if count := strings.Count(URL, timePathVar); count > 0 {
		URL = strings.Replace(URL, timePathVar, time.Format(pathTimeLayout), count)
	}
	return URL
}

func expandRetryURL(URL string, time time.Time, retry int) string {
	URL = expandURL(URL, time)
	ext := ""
	if extIndex := strings.Index(URL, "."); extIndex > -1 {
		ext = URL[extIndex:]
	}
	if index := strings.LastIndex(URL, RetryFragment); index > -1 {
		URL = URL[:index]
	} else {
		URL = URL[:len(URL)-len(ext)]
	}

	return URL + RetryFragment + fmt.Sprintf("%02d", retry+1) + ext
}
