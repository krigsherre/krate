package sketch

import (
	"fmt"
	"sync"
	"testing"
)

func TestNewCMS(t *testing.T) {
	cms := NewCMS(256, 4, 42)
	if cms.width != 256 {
		t.Errorf("width = %d, want 256", cms.width)
	}
	if cms.depth != 4 {
		t.Errorf("depth = %d, want 4", cms.depth)
	}
	if len(cms.table) != 4 {
		t.Fatalf("table rows = %d, want 4", len(cms.table))
	}
	for i := range cms.table {
		if len(cms.table[i]) != 256 {
			t.Fatalf("table[%d] length = %d, want 256", i, len(cms.table[i]))
		}
		for j, v := range cms.table[i] {
			if v != 0 {
				t.Errorf("table[%d][%d] = %d, want 0", i, j, v)
			}
		}
	}
	if len(cms.seeds) != 4 {
		t.Fatalf("seeds length = %d, want 4", len(cms.seeds))
	}
}

func TestAddQuery(t *testing.T) {
	cms := NewCMS(256, 4, 42)
	cms.Add("foo", 5)
	cms.Add("foo", 3)
	cms.Add("bar", 10)

	if got := cms.Query("foo"); got != 8 {
		t.Errorf("Query(foo) = %d, want 8", got)
	}
	if got := cms.Query("bar"); got != 10 {
		t.Errorf("Query(bar) = %d, want 10", got)
	}
}

func TestCMSNeverUnderestimates(t *testing.T) {
	cms := NewCMS(1024, 4, 42)
	counts := make(map[string]uint64)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		c := uint64(i + 1)
		cms.Add(key, c)
		counts[key] += c
	}

	for key, expected := range counts {
		got := cms.Query(key)
		if got < expected {
			t.Errorf("Query(%q) = %d < true count %d (underestimate!)", key, got, expected)
		}
	}
}

func TestCMSQueryUnknownKey(t *testing.T) {
	cms := NewCMS(256, 4, 42)
	if got := cms.Query("nonexistent"); got != 0 {
		t.Errorf("Query(nonexistent) = %d, want 0", got)
	}
}

func TestCMSReset(t *testing.T) {
	cms := NewCMS(256, 4, 42)
	cms.Add("foo", 5)
	cms.Add("bar", 10)

	cms.Reset()

	if got := cms.Query("foo"); got != 0 {
		t.Errorf("after Reset, Query(foo) = %d, want 0", got)
	}
	if got := cms.Query("bar"); got != 0 {
		t.Errorf("after Reset, Query(bar) = %d, want 0", got)
	}
}

func TestCMSClone(t *testing.T) {
	cms := NewCMS(256, 4, 42)
	cms.Add("foo", 5)

	clone := cms.Clone()

	cms.Add("foo", 10)
	if got := clone.Query("foo"); got != 5 {
		t.Errorf("clone.Query(foo) = %d, want 5", got)
	}
	if got := cms.Query("foo"); got != 15 {
		t.Errorf("original.Query(foo) = %d, want 15", got)
	}

	clone.Add("bar", 7)
	if got := cms.Query("bar"); got != 0 {
		t.Errorf("original.Query(bar) = %d, want 0 (clone mutation leaked)", got)
	}
}

func TestCMSMarshalRoundtrip(t *testing.T) {
	cms := NewCMS(256, 4, 42)

	data := cms.Marshal()
	got, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got.width != cms.width || got.depth != cms.depth {
		t.Errorf("dimensions: got %dx%d, want %dx%d", got.width, got.depth, cms.width, cms.depth)
	}
	if got.baseSeed != cms.baseSeed {
		t.Errorf("seed: got %d, want %d", got.baseSeed, cms.baseSeed)
	}

	for i := uint32(0); i < cms.depth; i++ {
		for j := uint32(0); j < cms.width; j++ {
			if got.table[i][j] != cms.table[i][j] {
				t.Fatalf("table[%d][%d]: got %d, want %d", i, j, got.table[i][j], cms.table[i][j])
			}
		}
	}
}

func TestCMSMarshalRoundtripWithKeys(t *testing.T) {
	cms := NewCMS(256, 4, 42)
	cms.Add("foo", 5)
	cms.Add("bar", 10)
	cms.Add("baz", 42)

	data := cms.Marshal()
	got, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	for _, key := range []string{"foo", "bar", "baz"} {
		want := cms.Query(key)
		if q := got.Query(key); q != want {
			t.Errorf("Query(%q) = %d, want %d", key, q, want)
		}
	}
}

func TestCMSMergeMax(t *testing.T) {
	cms1 := NewCMS(256, 4, 42)
	cms2 := NewCMS(256, 4, 42)

	cms1.Add("foo", 5)
	cms2.Add("foo", 10)
	cms2.Add("bar", 7)

	cms1.Merge(cms2, MergeMax)

	if got := cms1.Query("foo"); got != 10 {
		t.Errorf("Query(foo) = %d, want 10", got)
	}
	if got := cms1.Query("bar"); got != 7 {
		t.Errorf("Query(bar) = %d, want 7", got)
	}
}

func TestCMSMergeSum(t *testing.T) {
	cms1 := NewCMS(256, 4, 42)
	cms2 := NewCMS(256, 4, 42)

	cms1.Add("foo", 5)
	cms2.Add("foo", 10)

	cms1.Merge(cms2, MergeSum)

	if got := cms1.Query("foo"); got != 15 {
		t.Errorf("Query(foo) = %d, want 15", got)
	}
}

func TestCMSAccuracy(t *testing.T) {
	cms := NewCMS(1024, 4, 42)
	nKeys := 100

	for i := 0; i < nKeys; i++ {
		cms.Add(fmt.Sprintf("key-%d", i), 1)
	}

	var overcountSum uint64
	for i := 0; i < nKeys; i++ {
		est := cms.Query(fmt.Sprintf("key-%d", i))
		if est < 1 {
			t.Fatalf("underestimate: key-%d est=%d", i, est)
		}
		overcountSum += est - 1
	}

	avgOvercount := float64(overcountSum) / float64(nKeys)
	if avgOvercount > 1.0 {
		t.Errorf("average overcount = %.2f, want < 1.0", avgOvercount)
	}
}

func TestConcurrentAdd(t *testing.T) {
	cms := NewCMS(1024, 4, 42)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", n)
			for j := 0; j < 1000; j++ {
				cms.Add(key, 1)
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		if got := cms.Query(key); got != 1000 {
			t.Errorf("Query(%q) = %d, want 1000", key, got)
		}
	}
}
