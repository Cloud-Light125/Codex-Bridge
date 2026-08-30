package numberprefix

import (
	"fmt"
	"strconv"
	"strings"
)

type Prefix struct {
	Number   int
	Content  string
	Explicit bool
}

// Parse recognizes #12, [12], and the historical bare 12 prefix without
// deciding which backend owns the number. The caller supplies that lookup so
// Codex and OpenClaw can never share a number namespace accidentally.
func Parse(text string) (Prefix, bool, error) {
	trimmed := strings.TrimSpace(text)
	numberText, rest, explicit := "", "", false
	switch {
	case strings.HasPrefix(trimmed, "#"):
		explicit = true
		numberText, rest = takeNumber(trimmed[1:])
	case strings.HasPrefix(trimmed, "["):
		if close := strings.Index(trimmed, "]"); close > 1 {
			explicit = true
			numberText, rest = strings.TrimSpace(trimmed[1:close]), strings.TrimSpace(trimmed[close+1:])
		}
	default:
		numberText, rest = takeNumber(trimmed)
	}
	if numberText == "" {
		return Prefix{Content: text, Explicit: explicit}, explicit, nil
	}
	number, err := strconv.Atoi(numberText)
	if err != nil || number < 1 {
		if explicit {
			return Prefix{}, true, fmt.Errorf("无效的聊天编号 #%s", numberText)
		}
		return Prefix{}, false, nil
	}
	return Prefix{Number: number, Content: strings.TrimSpace(rest), Explicit: explicit}, true, nil
}

func takeNumber(value string) (string, string) {
	value = strings.TrimLeft(value, " \t")
	index := 0
	for index < len(value) && value[index] >= '0' && value[index] <= '9' {
		index++
	}
	if index == 0 {
		return "", ""
	}
	if index < len(value) && value[index] != ' ' && value[index] != '\t' && value[index] != '\r' && value[index] != '\n' {
		return "", ""
	}
	return value[:index], strings.TrimSpace(value[index:])
}
