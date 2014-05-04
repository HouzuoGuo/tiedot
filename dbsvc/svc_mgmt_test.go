package dbsvc

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func touchFile(dir, filename string) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		panic(err)
	}
	if err := ioutil.WriteFile(path.Join(dir, filename), make([]byte, 0), 0600); err != nil {
		panic(err)
	}
}

func SchemaTest(t *testing.T) {
	if mkIndexUID("a", []string{"b", "c"}) != "a!b!c" {
		t.Fatal()
	}
	if colName, idxPath := destructIndexUID("a!b!c_d"); colName != "a" || idxPath[0] != "b" || idxPath[1] != "c_d" {
		t.Fatal(colName, idxPath)
	}

	touchFile(TEST_DATA_DIR+"/ColA_1", "dat_0")
	touchFile(TEST_DATA_DIR+"/ColA_1", "id_0")
	touchFile(TEST_DATA_DIR+"/ColA_1/ht_a!b_c", "0")
	if err := db.LoadSchema(false); err == nil {
		t.Fatal("Should have thrown error") // partition number mismatch
	}
	os.RemoveAll(TEST_DATA_DIR + "/ColA_1")

	touchFile(TEST_DATA_DIR+"/ColA_2", "dat_0")
	touchFile(TEST_DATA_DIR+"/ColA_2", "id_0")
	touchFile(TEST_DATA_DIR+"/ColA_2", "dat_1")
	touchFile(TEST_DATA_DIR+"/ColA_2", "id_1")
	touchFile(TEST_DATA_DIR+"/ColA_2/ht_a!b_c", "0")
	touchFile(TEST_DATA_DIR+"/ColA_2/ht_a!b_c", "1")
	var schemaVer1, schemaVer2, schemaVer3 int64
	schemaVer1 = db.mySchemaVersion
	if err := db.LoadSchema(false); err != nil {
		t.Fatal(err)
	}
	schemaVer2 = db.mySchemaVersion
	if schemaVer2 < schemaVer1 {
		t.Fatal(schemaVer2, schemaVer1)
	}
	if err := db.LoadSchema(true); err != nil {
		t.Fatal(err)
	}
	schemaVer3 = db.mySchemaVersion
	if schemaVer3 < schemaVer2 {
		t.Fatal(schemaVer3, schemaVer2)
	}
	if _, exists := db.schema["ColA"]; !exists {
		t.Fatal(db.schema)
	}
	if _, exists := db.schema["ColA"]["ColA!a!b_c"]; !exists ||
		db.schema["ColA"]["ColA!a!b_c"][0] != "a" ||
		db.schema["ColA"]["ColA!a!b_c"][1] != "b_c" {
		t.Fatal(db.schema)
	}
}
