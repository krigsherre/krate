package sketch

import "testing"

func TestCompressDecompressRoundtrip(t *testing.T) {
	cms := NewCMS(256, 4, 42)
	cms.Add("foo", 5)
	cms.Add("bar", 10)
	cms.Add("baz", 42)

	raw := cms.Marshal()
	compressed := Compress(raw)

	decompressed, err := Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}

	got, err := Unmarshal(decompressed)
	if err != nil {
		t.Fatalf("Unmarshal decompressed: %v", err)
	}

	if got.width != cms.width || got.depth != cms.depth {
		t.Errorf("dimensions: got %dx%d, want %dx%d", got.width, got.depth, cms.width, cms.depth)
	}

	for _, key := range []string{"foo", "bar", "baz"} {
		want := cms.Query(key)
		if q := got.Query(key); q != want {
			t.Errorf("Query(%q) = %d, want %d", key, q, want)
		}
	}
}

func TestCompressSize(t *testing.T) {
	cms := NewCMS(256, 4, 42)
	cms.Add("key1", 1)
	cms.Add("key2", 2)
	cms.Add("key3", 3)

	raw := cms.Marshal()
	compressed := Compress(raw)

	t.Logf("raw: %d bytes, compressed: %d bytes (%.1f%% of raw)",
		len(raw), len(compressed), 100*float64(len(compressed))/float64(len(raw)))

	if len(compressed) >= len(raw) {
		t.Errorf("compressed (%d) >= raw (%d); compression ineffective", len(compressed), len(raw))
	}

	if len(compressed) > 1200 {
		t.Errorf("compressed size = %d, want <= 1200 for sparse 256×4 CMS", len(compressed))
	}
}

func TestDecompressInvalidData(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"nil", nil},
		{"too_short", []byte("CM")},
		{"bad_magic", []byte("XX\x01\x00\x00\x00\x04\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x2a")},
		{"garbage", []byte("garbage data here")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Decompress(tc.data)
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

func TestCompressEmptyCMS(t *testing.T) {
	cms := NewCMS(256, 4, 42)

	raw := cms.Marshal()
	compressed := Compress(raw)

	decompressed, err := Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}

	got, err := Unmarshal(decompressed)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	for _, key := range []string{"foo", "bar", "anything"} {
		if q := got.Query(key); q != 0 {
			t.Errorf("Query(%q) = %d, want 0", key, q)
		}
	}

	rebuilt := got.Marshal()
	if len(raw) != len(rebuilt) {
		t.Fatalf("length mismatch: %d vs %d", len(raw), len(rebuilt))
	}
	for i := range raw {
		if raw[i] != rebuilt[i] {
			t.Fatalf("byte mismatch at offset %d: %02x vs %02x", i, raw[i], rebuilt[i])
		}
	}
}
