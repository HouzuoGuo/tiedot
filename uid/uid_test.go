package uid

import (
	"encoding/json"
	"testing"
)

func TestNextUID(t *testing.T) {
	for i := 0; i < 1000; i++ {
		next := NextUID()
		if next == 0 {
			t.Fatal(next)
		}
	}
}

func TestGetPKOfDoc(t *testing.T) {
	var doc1, doc2, doc3 map[string]interface{}
	json.Unmarshal([]byte(`{"@id": 1}`), &doc1)
	json.Unmarshal([]byte(`{"@id": "a"}`), &doc2)
	json.Unmarshal([]byte(`{"@id": "1"}`), &doc3)

	if _, found := PKOfDoc(doc1); found {
		t.Fatal(doc1)
	}

	if _, found := PKOfDoc(doc2); found {
		t.Fatal(doc2)
	}

	if uid, found := PKOfDoc(doc3); !found || uid != 1 {
		t.Fatal(doc3)
	}
}

func BenchmarkUID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		NextUID()
	}
}
