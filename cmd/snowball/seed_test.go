package main

import (
	"regexp"
	"strings"
	"testing"
	"wloc/lib/mac"
)

var macRe = regexp.MustCompile(`^[0-9a-f]{2}(:[0-9a-f]{2}){5}$`)

func TestParseOUIs(t *testing.T) {
	in := "E005C5\tTP-LINK TECHNOLOGIES CO.,LTD.\nA0F3C1\tTP-LINK TECHNOLOGIES CO.,LTD.\n\n"
	got, err := parseOUIs(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"e005c5", "a0f3c1"}
	if len(got) != len(want) {
		t.Fatalf("got %d OUIs, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("OUI[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestParseOUIsSkipsMalformed(t *testing.T) {
	in := "E005C5\tVendor\nZZZ\tbad\n12\tshort\n"
	got, err := parseOUIs(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "e005c5" {
		t.Fatalf("expected only the valid OUI, got %v", got)
	}
}

func TestRandomBSSIDWellFormed(t *testing.T) {
	ouis := []string{"e005c5", "a0f3c1"}
	for i := 0; i < 200; i++ {
		b := randomBSSID(ouis)
		if !macRe.MatchString(b) {
			t.Fatalf("malformed BSSID: %q", b)
		}
		// Prefix must be one of the supplied OUIs.
		prefix := strings.ReplaceAll(b, ":", "")[:6]
		if prefix != "e005c5" && prefix != "a0f3c1" {
			t.Fatalf("BSSID %q has prefix %q not in OUI list", b, prefix)
		}
		// Must round-trip through the project's MAC encoder.
		if _, err := mac.Encode(b); err != nil {
			t.Fatalf("mac.Encode(%q) failed: %v", b, err)
		}
	}
}

func TestEmbeddedOUIsLoad(t *testing.T) {
	ouis, err := loadOUIs()
	if err != nil {
		t.Fatal(err)
	}
	if len(ouis) < 1000 {
		t.Fatalf("expected the embedded OUI list to load many entries, got %d", len(ouis))
	}
}
