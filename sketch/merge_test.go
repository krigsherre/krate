package sketch

import "testing"

func TestMergeMax(t *testing.T) {
	cms1 := NewCMS(256, 4, 42)
	cms2 := NewCMS(256, 4, 42)

	cms1.Add("a", 5)
	cms1.Add("b", 3)
	cms2.Add("a", 10)
	cms2.Add("b", 1)
	cms2.Add("c", 7)

	cms1.Merge(cms2, MergeMax)

	if got := cms1.Query("a"); got != 10 {
		t.Errorf("Query(a) = %d, want 10", got)
	}
	if got := cms1.Query("b"); got != 3 {
		t.Errorf("Query(b) = %d, want 3", got)
	}
	if got := cms1.Query("c"); got != 7 {
		t.Errorf("Query(c) = %d, want 7", got)
	}
}

func TestMergeMin(t *testing.T) {
	cms1 := NewCMS(256, 4, 42)
	cms2 := NewCMS(256, 4, 42)

	cms1.Add("a", 10)
	cms1.Add("b", 3)
	cms2.Add("a", 5)
	cms2.Add("b", 1)
	cms2.Add("c", 7)

	cms1.Merge(cms2, MergeMin)

	if got := cms1.Query("a"); got != 5 {
		t.Errorf("Query(a) = %d, want 5", got)
	}
	if got := cms1.Query("b"); got != 1 {
		t.Errorf("Query(b) = %d, want 1", got)
	}
	if got := cms1.Query("c"); got != 0 {
		t.Errorf("Query(c) = %d, want 0", got)
	}
}

func TestMergeSum(t *testing.T) {
	cms1 := NewCMS(256, 4, 42)
	cms2 := NewCMS(256, 4, 42)

	cms1.Add("a", 5)
	cms2.Add("a", 10)

	cms1.Merge(cms2, MergeSum)

	if got := cms1.Query("a"); got != 15 {
		t.Errorf("Query(a) = %d, want 15", got)
	}
}

func TestMergeReplace(t *testing.T) {
	cms1 := NewCMS(256, 4, 42)
	cms2 := NewCMS(256, 4, 42)

	cms1.Add("a", 5)
	cms1.Add("b", 3)
	cms2.Add("a", 10)

	cms1.Merge(cms2, MergeReplace)

	if got := cms1.Query("a"); got != 10 {
		t.Errorf("Query(a) = %d, want 10", got)
	}
	if got := cms1.Query("b"); got != 0 {
		t.Errorf("Query(b) = %d, want 0", got)
	}
}

func TestMergeDimensionMismatch(t *testing.T) {
	cms1 := NewCMS(256, 4, 42)
	cms2 := NewCMS(128, 4, 42)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on dimension mismatch, got none")
		}
	}()

	cms1.Merge(cms2, MergeMax)
}
