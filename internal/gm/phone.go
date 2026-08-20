package gm

import (
	"fmt"
	"strings"
	"unicode"
)

// NormalizePhone strips formatting and returns an E.164-ish number.
//
// Bare 10-digit US numbers get a +1 prefix. Numbers that already include a
// leading + keep their country code. Conversation IDs in this protocol are
// short numeric strings (often 3–6 digits), so anything under 10 digits is
// rejected — those must be passed as --to, not --phone.
func NormalizePhone(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("phone number is required")
	}

	var b strings.Builder
	b.Grow(len(trimmed) + 1)
	wrotePlus := false
	for _, r := range trimmed {
		if r == '+' && !wrotePlus && b.Len() == 0 {
			b.WriteByte('+')
			wrotePlus = true
			continue
		}
		if unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}

	out := b.String()
	digits := strings.TrimPrefix(out, "+")
	if len(digits) < 10 {
		return "", fmt.Errorf("phone %q is too short after normalizing (%d digits); conversation ids are not phone numbers — use --to for those", raw, len(digits))
	}
	if strings.HasPrefix(out, "+") {
		return "+" + digits, nil
	}
	if len(digits) == 10 {
		return "+1" + digits, nil
	}
	if len(digits) == 11 && digits[0] == '1' {
		return "+" + digits, nil
	}
	return "+" + digits, nil
}
