package addressutil

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
)

const testTonBounceable = "EQC6KV4zs8TJtSZapOrRFmqSkxzpq-oSCoxekQRKElf4nC1I"

func TestParseTonMainnetAddressNormalizesSupportedFormats(t *testing.T) {
	base := address.MustParseAddr(testTonBounceable)
	raw := base.StringRaw()
	nonBounce := base.Bounce(false).String()
	rawBytes, err := base64.RawURLEncoding.DecodeString(testTonBounceable)
	if err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	stdBase64 := base64.RawStdEncoding.EncodeToString(rawBytes)

	for _, input := range []string{testTonBounceable, nonBounce, raw, stdBase64} {
		parsed, err := ParseTonMainnetAddress(input)
		if err != nil {
			t.Fatalf("ParseTonMainnetAddress(%q): %v", input, err)
		}
		if parsed.Display != nonBounce {
			t.Fatalf("display for %q = %q, want %q", input, parsed.Display, nonBounce)
		}
		if parsed.Raw != strings.ToLower(raw) {
			t.Fatalf("raw for %q = %q, want %q", input, parsed.Raw, strings.ToLower(raw))
		}
	}
}

func TestParseTonMainnetAddressRejectsTestnetOnly(t *testing.T) {
	testnet := address.MustParseAddr(testTonBounceable).Testnet(true).String()
	if _, err := ParseTonMainnetAddress(testnet); err == nil {
		t.Fatal("expected testnet-only address to be rejected")
	}
}

func TestTonAddressFromShortAccount(t *testing.T) {
	base := address.MustParseAddr(testTonBounceable)
	got := TonAddressFromShortAccount(base.Workchain(), base.Data())
	if got == nil {
		t.Fatal("TonAddressFromShortAccount() returned nil")
	}
	if TonRawAddressObjectKey(got) != TonRawAddressObjectKey(base) {
		t.Fatalf("raw address = %q, want %q", TonRawAddressObjectKey(got), TonRawAddressObjectKey(base))
	}
	if got = TonAddressFromShortAccount(base.Workchain(), base.Data()[:31]); got != nil {
		t.Fatalf("TonAddressFromShortAccount() with short account = %#v, want nil", got)
	}
}

// TestTonDisplayAndRawKeyAreDifferentForms pins the relationship between the
// two TON normalizers on a real mainnet address (the USDT jetton master).
// Orders store NormalizeTonAddress output; transaction locks store
// TonRawAddressKey output. They are different strings for the same account,
// so a column written with one can never be compared with a value produced by
// the other.
func TestTonDisplayAndRawKeyAreDifferentForms(t *testing.T) {
	const usdtMaster = "EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs"
	const wantDisplay = "UQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_p0p"
	const wantRaw = "0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe"

	inputs := []string{
		usdtMaster,
		wantDisplay,
		wantRaw,
		strings.ToUpper(wantRaw),
		"  " + usdtMaster + "  ",
	}
	for _, input := range inputs {
		display, err := NormalizeTonAddress(input)
		if err != nil {
			t.Fatalf("NormalizeTonAddress(%q): %v", input, err)
		}
		raw, err := TonRawAddressKey(input)
		if err != nil {
			t.Fatalf("TonRawAddressKey(%q): %v", input, err)
		}
		if display != wantDisplay {
			t.Fatalf("NormalizeTonAddress(%q) = %q, want %q", input, display, wantDisplay)
		}
		if raw != wantRaw {
			t.Fatalf("TonRawAddressKey(%q) = %q, want %q", input, raw, wantRaw)
		}
		if display == raw {
			t.Fatalf("display and raw forms unexpectedly equal for %q", input)
		}
		// Each form converts losslessly into the other.
		if back, err := TonRawAddressKey(display); err != nil || back != raw {
			t.Fatalf("TonRawAddressKey(display %q) = %q, %v; want %q", display, back, err, raw)
		}
		if back, err := NormalizeTonAddress(raw); err != nil || back != display {
			t.Fatalf("NormalizeTonAddress(raw %q) = %q, %v; want %q", raw, back, err, display)
		}
	}
}
