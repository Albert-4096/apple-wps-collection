package main

import (
	"bufio"
	_ "embed"
	"fmt"
	"io"
	"math/rand"
	"strings"
)

//go:embed top_ouis.txt
var ouiData string

var ouiPrefixRe = mustHexPrefix()

// parseOUIs reads a `PREFIX<TAB>VENDOR` list (the IEEE OUI format used by
// top_ouis.txt) and returns the lower-cased 6-hex-digit prefixes. Malformed
// lines are skipped.
func parseOUIs(r io.Reader) ([]string, error) {
	var ouis []string
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		prefix := strings.ToLower(strings.Fields(line)[0])
		if len(prefix) != 6 || !ouiPrefixRe(prefix) {
			continue
		}
		ouis = append(ouis, prefix)
	}
	return ouis, scanner.Err()
}

// loadOUIs parses the embedded OUI list.
func loadOUIs() ([]string, error) {
	return parseOUIs(strings.NewReader(ouiData))
}

// randomBSSID picks a random real OUI prefix and appends a random 24-bit
// suffix, yielding a canonical lowercase MAC string (aa:bb:cc:dd:ee:ff).
func randomBSSID(ouis []string) string {
	prefix := ouis[rand.Intn(len(ouis))]
	suffix := rand.Intn(1 << 24)
	return fmt.Sprintf("%s:%s:%s:%02x:%02x:%02x",
		prefix[0:2], prefix[2:4], prefix[4:6],
		byte(suffix>>16), byte(suffix>>8), byte(suffix))
}

func mustHexPrefix() func(string) bool {
	const hexits = "0123456789abcdef"
	return func(s string) bool {
		for _, c := range s {
			if !strings.ContainsRune(hexits, c) {
				return false
			}
		}
		return true
	}
}
