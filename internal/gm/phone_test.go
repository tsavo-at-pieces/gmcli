package gm

import "testing"

func TestNormalizePhone(t *testing.T) {
	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "(202) 555-0142", want: "+12025550142"},
		{in: "2025550142", want: "+12025550142"},
		{in: "+1 202 555 0142", want: "+12025550142"},
		{in: "12025550142", want: "+12025550142"},
		{in: "+447911123456", want: "+447911123456"},
		{in: "2330", wantErr: true},
		{in: "156", wantErr: true},
		{in: "", wantErr: true},
		{in: "   ", wantErr: true},
	}
	for _, tt := range tests {
		got, err := NormalizePhone(tt.in)
		if tt.wantErr {
			if err == nil {
				t.Errorf("NormalizePhone(%q) = %q, want error", tt.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("NormalizePhone(%q) unexpected error: %v", tt.in, err)
			continue
		}
		if got != tt.want {
			t.Errorf("NormalizePhone(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
