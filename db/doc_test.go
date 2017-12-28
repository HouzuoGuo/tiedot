package db

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/HouzuoGuo/tiedot/data"
	"github.com/HouzuoGuo/tiedot/dberr"
	"github.com/bouk/monkey"
	"log"
	"reflect"
)

var (
	tempDir = "./tmp"
)

func TestStrHash(t *testing.T) {
	listStr := []string{"", " ", "abc", "123"}
	hashes := []int{0, 32, 417419622498, 210861491250}
	for i := range listStr {
		if StrHash(listStr[i]) != hashes[i] {
			t.Fatalf("Hash of %s equals to %d, it should equal to %d", listStr[i], StrHash(listStr[i]), hashes[i])
		}
	}
}
func GetInTest(t *testing.T) {
	var obj interface{}
	// Get inside a JSON object
	json.Unmarshal([]byte(`{"a": {"b": {"c": 1}}}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 1 {
		t.Fatal()
	}
	// Get inside a JSON array
	json.Unmarshal([]byte(`{"a": {"b": {"c": [1, 2, 3]}}}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 1 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[1].(float64); !ok || val != 2 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[2].(float64); !ok || val != 3 {
		t.Fatal()
	}
	// Get inside JSON objects contained in JSON array
	json.Unmarshal([]byte(`{"a": [{"b": {"c": [1]}}, {"b": {"c": [2, 3]}}]}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 1 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[1].(float64); !ok || val != 2 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[2].(float64); !ok || val != 3 {
		t.Fatal()
	}
	// Get inside a JSON array and fetch attributes from array elements, which are JSON objects
	json.Unmarshal([]byte(`{"a": [{"b": {"c": [4]}}, {"b": {"c": [5, 6]}}], "d": [0, 9]}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 4 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[1].(float64); !ok || val != 5 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[2].(float64); !ok || val != 6 {
		t.Fatal()
	}
	if len(GetIn(obj, []string{"a", "b", "c"})) != 3 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"d"})[0].(float64); !ok || val != 0 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"d"})[1].(float64); !ok || val != 9 {
		t.Fatal()
	}
	if len(GetIn(obj, []string{"d"})) != 2 {
		t.Fatal()
	}
	// Another example
	json.Unmarshal([]byte(`{"a": {"b": [{"c": 2}]}, "d": 0}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 2 {
		t.Fatal()
	}
	if len(GetIn(obj, []string{"a", "b", "c"})) != 1 {
		t.Fatal()
	}
}
func idxHas(col *Col, path []string, idxVal interface{}, docID int) error {
	idxName := strings.Join(path, INDEX_PATH_SEP)
	hashKey := StrHash(fmt.Sprint(idxVal))
	vals := col.hts[hashKey%col.db.numParts][idxName].Get(hashKey, 0)
	if len(vals) != 1 || vals[0] != docID {
		return fmt.Errorf("Looking for %v (%v) docID %v in %v partition %d, but got result %v", idxVal, hashKey, docID, path, hashKey%col.db.numParts, vals)
	}
	return nil
}
func idxHasNot(col *Col, path []string, idxVal, docID int) error {
	idxName := strings.Join(path, INDEX_PATH_SEP)
	hashKey := StrHash(fmt.Sprint(idxVal))
	vals := col.hts[hashKey%col.db.numParts][idxName].Get(hashKey, 0)
	for _, v := range vals {
		if v == docID {
			return fmt.Errorf("Looking for %v %v %v in %v (should not return any), but got result %v", idxVal, hashKey, docID, path, vals)
		}
	}
	return nil
}
func TestDocCrudAndIdx(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	if err := os.MkdirAll(TEST_DATA_DIR, 0700); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(TEST_DATA_DIR+"/number_of_partitions", []byte("2"), 0600); err != nil {
		t.Fatal(err)
	}
	db, err := OpenDB(TEST_DATA_DIR)
	if err != nil {
		t.Fatal(err)
	}
	// Prepare collection and index
	if err = db.Create("col"); err != nil {
		t.Fatal(err)
	}
	col := db.Use("col")
	if err = col.Index([]string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	numDocs := 2011
	docIDs := make([]int, numDocs)
	// Insert documents
	for i := 0; i < numDocs; i++ {
		if docIDs[i], err = col.Insert(map[string]interface{}{"a": map[string]interface{}{"b": i}}); err != nil {
			t.Fatal(err)
		}
	}
	// Read documents and verify index
	if _, err = col.Read(123456); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("Did not error")
	}
	for i, docID := range docIDs {
		if doc, err := col.Read(docID); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i) {
			t.Fatal(docID, doc)
		}
		if err = idxHas(col, []string{"a", "b"}, i, docID); err != nil {
			t.Fatal(err)
		}
	}
	// Update document
	if err = col.Update(654321, map[string]interface{}{}); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("Did not error")
	}
	for i, docID := range docIDs {
		// i -> i * 2
		if err = col.Update(docID, map[string]interface{}{"a": map[string]interface{}{"b": i * 2}}); err != nil {
			t.Fatal(err)
		}
	}
	// After update - verify documents and index
	for i, docID := range docIDs {
		if doc, err := col.Read(docID); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i*2) {
			t.Fatal(docID, doc)
		}
		if i == 0 {
			if err = idxHas(col, []string{"a", "b"}, 0, docID); err != nil {
				t.Fatal(err)
			}
		} else {
			if err = idxHasNot(col, []string{"a", "b"}, i, docID); err != nil {
				t.Fatal(err)
			}
			if err = idxHas(col, []string{"a", "b"}, i*2, docID); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Delete half of those documents
	if err = col.Delete(654321); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("Did not error")
	}
	for i := 0; i < numDocs/2+1; i++ {
		if err := col.Delete(docIDs[i]); err != nil {
			t.Fatal(err)
		}
		if err := col.Delete(docIDs[i]); dberr.Type(err) != dberr.ErrorNoDoc {
			t.Fatal("Did not error")
		}
	}
	// After delete - verify
	for i, docID := range docIDs {
		if i < numDocs/2+1 {
			// After delete - verify deleted documents and index
			if _, err := col.Read(docID); dberr.Type(err) != dberr.ErrorNoDoc {
				t.Fatal("Did not delete", i, docID)
			}
			if err = idxHasNot(col, []string{"a", "b"}, i*2, docID); err != nil {
				t.Fatal(err)
			}
		} else {
			// After delete - verify unaffected documents and index
			if doc, err := col.Read(docID); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i*2) {
				t.Fatal(docID, doc)
			}
			if err = idxHas(col, []string{"a", "b"}, i*2, docID); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Recreate index and verify
	if err = col.Unindex([]string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if err = col.Index([]string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	for i := numDocs/2 + 1; i < numDocs; i++ {
		if doc, err := col.Read(docIDs[i]); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i*2) {
			t.Fatal(doc, err)
		}
		if err = idxHas(col, []string{"a", "b"}, i*2, docIDs[i]); err != nil {
			t.Fatal(err)
		}
	}

	// Verify that there are approximately 1000 documents
	t.Log("ApproxDocCount", col.ApproxDocCount())
	if col.ApproxDocCount() < 600 || col.ApproxDocCount() > 1400 {
		t.Fatal("Approximate is way off", col.ApproxDocCount())
	}

	// Scrub and verify
	if err = db.Scrub("col"); err != nil {
		t.Fatal(err)
	}
	col = db.Use("col")
	for i := numDocs/2 + 1; i < numDocs; i++ {
		if doc, err := col.Read(docIDs[i]); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i*2) {
			t.Fatal(doc, err)
		}
		if err = idxHas(col, []string{"a", "b"}, i*2, docIDs[i]); err != nil {
			t.Fatal(err)
		}
	}

	// Iterate over all documents 10 times
	start := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		col.ForEachDoc(func(_ int, _ []byte) bool {
			return true
		})
	}
	timediff := time.Now().UnixNano() - start
	t.Log("It took", timediff/1000000, "milliseconds")

	// Verify again that there are approximately 1000 documents
	t.Log("ApproxDocCount", col.ApproxDocCount())
	if col.ApproxDocCount() < 600 || col.ApproxDocCount() > 1400 {
		t.Fatal("Approximate is way off", col.ApproxDocCount())
	}

	// Read back all documents page by page
	totalPage := col.ApproxDocCount() / 100
	collectedIDs := make(map[int]struct{})
	for page := 0; page < totalPage; page++ {
		col.ForEachDocInPage(page, totalPage, func(id int, _ []byte) bool {
			collectedIDs[id] = struct{}{}
			return true
		})
		t.Log("Went through page ", page, " got ", len(collectedIDs), " documents so far")
	}
	if len(collectedIDs) != numDocs/2 {
		t.Fatal("Wrong number of docs", len(collectedIDs))
	}

	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestUpdateFunc(t *testing.T) {
	fatalIf := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}

	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	err := os.MkdirAll(TEST_DATA_DIR, 0700)
	fatalIf(err)
	err = ioutil.WriteFile(TEST_DATA_DIR+"/number_of_partitions", []byte("2"), 0600)
	fatalIf(err)
	db, err := OpenDB(TEST_DATA_DIR)
	fatalIf(err)
	// Prepare collection
	err = db.Create("col")
	fatalIf(err)
	col := db.Use("col")
	// end of setup section

	type myData struct {
		Num int
		Txt string
	}
	conv := func(x myData) map[string]interface{} {
		xStr, err := json.Marshal(x)
		fatalIf(err)
		var xUnmarshaled map[string]interface{}
		err = json.Unmarshal([]byte(xStr), &xUnmarshaled)
		fatalIf(err)
		return xUnmarshaled
	}
	incNumBytes := func(doc []byte) ([]byte, error) {
		if rand.Intn(100) == 0 {
			time.Sleep(10)
		}

		if !bytes.Contains(doc, []byte(`Num":`)) {
			return nil, errors.New("bytes does not contains num")
		}

		pos := bytes.IndexAny(doc, "1234567890")
		end := pos + bytes.IndexAny(doc[pos:], " ,}")
		num, err := strconv.Atoi(string(doc[pos:end]))
		num++
		numB := []byte(strconv.Itoa(num))
		return append(doc[:pos], append(numB, doc[end:]...)...), err
	}
	incNumDoc := func(doc map[string]interface{}) (map[string]interface{}, error) {
		num, ok := doc["Num"].(float64)
		if !ok {
			return nil, errors.New("doc does not contain num")
		}
		num++
		return map[string]interface{}{
			"Txt": doc["Txt"],
			"Num": num,
		}, nil
	}

	id, err := col.Insert(conv(myData{Num: 3, Txt: "some other data"}))
	fatalIf(err)

	const N = 1000
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			var err error
			if i%2 == 0 {
				err = col.UpdateBytesFunc(id, incNumBytes)
			} else {
				err = col.UpdateFunc(id, incNumDoc)
			}
			fatalIf(err)
			wg.Done()
		}(i)
	}
	wg.Wait()
	doc, err := col.Read(id)
	fatalIf(err)
	num, ok := doc["Num"].(float64)
	if doc["Txt"] != "some other data" || !ok || num != 3+N {
		t.Fatal("unexpected result")
	}

	err = db.Close()
	fatalIf(err)
}
func TestUpdate(t *testing.T) {
	fatalIf := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}

	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	err := os.MkdirAll(TEST_DATA_DIR, 0700)
	fatalIf(err)
	err = ioutil.WriteFile(TEST_DATA_DIR+"/number_of_partitions", []byte("2"), 0600)
	fatalIf(err)
	db, err := OpenDB(TEST_DATA_DIR)
	fatalIf(err)
	err = db.Create("col")
	fatalIf(err)
	col := db.Use("col")

	id, err := col.Insert(map[string]interface{}{"a": "x"})
	fatalIf(err)
	const N = 1000
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			err := col.Update(id, map[string]interface{}{"a": "x"})
			wg.Done()
			fatalIf(err)
		}()
	}
	wg.Wait()
	doc, err := col.Read(id)
	fatalIf(err)
	if doc["a"] != "x" {
		t.Fatal("unexpected result")
	}

	err = db.Close()
	fatalIf(err)
}
func TestGetInTypeСonversionErr(t *testing.T) {
	GetIn("typeError", []string{})
}
func TestGetInPathIsEpmty(t *testing.T) {
	if len(GetIn(map[string]interface{}{}, []string{"a", "b", "c"})) != 0 {
		t.Error("Expected value is empty")
	}
}
func TestColInsertRecoveryMarshalJsErr(t *testing.T) {
	db, _ := OpenDB(tempDir)
	defer os.RemoveAll(tempDir)

	col, _ := OpenCol(db, "test")
	errMessage := "Error json marshal"
	patch := monkey.Patch(json.Marshal, func(v interface{}) ([]byte, error) {
		return nil, errors.New(errMessage)
	})
	defer patch.Unpatch()
	col.InsertRecovery(0, map[string]interface{}{"test": "fail json"})
}
func TestColInsertRecoveryInsertErr(t *testing.T) {
	var (
		part *data.Partition
	)
	db, _ := OpenDB(tempDir)
	defer os.RemoveAll(tempDir)

	col, _ := OpenCol(db, "test")
	errMessage := "Insert error"
	patch := monkey.PatchInstanceMethod(reflect.TypeOf(part), "Insert", func(_ *data.Partition, id int, data []byte) (physID int, err error) {
		return 0, errors.New(errMessage)
	})
	defer patch.Unpatch()

	if col.InsertRecovery(0, map[string]interface{}{"test": "fail json"}).Error() != errMessage {
		t.Errorf("Expected error : %s", errMessage)
	}
}
func TestInsertErr(t *testing.T) {
	var part *data.Partition
	db, _ := OpenDB(tempDir)
	defer os.RemoveAll(tempDir)

	col, _ := OpenCol(db, "test")
	errMessage := "Insert error"
	patch := monkey.PatchInstanceMethod(reflect.TypeOf(part), "Insert", func(_ *data.Partition, id int, data []byte) (physID int, err error) {
		return 0, errors.New(errMessage)
	})
	defer patch.Unpatch()

	if _, err := col.Insert(map[string]interface{}{"test": "fail json"}); err.Error() != errMessage {
		t.Errorf("Expected error : %s", errMessage)
	}
}
func TestInsertJsMarshalErr(t *testing.T) {
	db, _ := OpenDB(tempDir)
	defer os.RemoveAll(tempDir)

	col, _ := OpenCol(db, "test")
	errMessage := "Error json marshal"
	patch := monkey.Patch(json.Marshal, func(v interface{}) ([]byte, error) {
		return nil, errors.New(errMessage)
	})
	defer patch.Unpatch()
	if _, err := col.Insert(map[string]interface{}{"test": "fail json"}); err.Error() != errMessage {
		t.Errorf("Expected error : %s", errMessage)
	}
}
func TestUpdateDocIsNill(t *testing.T) {
	db, _ := OpenDB(tempDir)
	defer os.RemoveAll(tempDir)

	col, _ := OpenCol(db, "test")

	if col.Update(0, nil).Error() != fmt.Sprintf("Updating %d: input doc may not be nil", 0) {
		t.Error("Expected error input map is nill")
	}
}
func TestUpdateJsMarshalErr(t *testing.T) {
	db, _ := OpenDB(tempDir)
	defer os.RemoveAll(tempDir)

	col, _ := OpenCol(db, "test")
	errMessage := "Error json marshal"
	patch := monkey.Patch(json.Marshal, func(v interface{}) ([]byte, error) {
		return nil, errors.New(errMessage)
	})
	defer patch.Unpatch()
	if col.Update(0, map[string]interface{}{"test": "fail json"}).Error() != errMessage {
		t.Errorf("Expected error : %s", errMessage)
	}
}
func TestUpdatePartError(t *testing.T) {
	var part *data.Partition
	db, _ := OpenDB(tempDir)
	defer os.RemoveAll(tempDir)

	col, _ := OpenCol(db, "test")
	errMessage := "Update error"
	patchUpdate := monkey.PatchInstanceMethod(reflect.TypeOf(part), "Update", func(_ *data.Partition, id int, data []byte) (err error) {
		return errors.New(errMessage)
	})
	patchRead := monkey.PatchInstanceMethod(reflect.TypeOf(part), "Read", func(_ *data.Partition, id int) ([]byte, error) {
		return []byte{}, nil
	})
	defer patchUpdate.Unpatch()
	defer patchRead.Unpatch()

	if col.Update(0, map[string]interface{}{"test": "fail json"}).Error() != errMessage {
		t.Errorf("Expected error : %s", errMessage)
	}
}
func TestUpdateAttemptDoc(t *testing.T) {
	db, _ := OpenDB(tempDir)
	defer os.RemoveAll(tempDir)

	col, _ := OpenCol(db, "test")
	var (
		part *data.Partition
		buf  bytes.Buffer
	)
	log.SetOutput(&buf)
	patchUpdate := monkey.PatchInstanceMethod(reflect.TypeOf(part), "Update", func(_ *data.Partition, id int, data []byte) (err error) {
		return nil
	})
	patchRead := monkey.PatchInstanceMethod(reflect.TypeOf(part), "Read", func(_ *data.Partition, id int) ([]byte, error) {
		return []byte{}, nil
	})
	patchUnmarshalJs := monkey.Patch(json.Unmarshal, func(data []byte, v interface{}) error {
		return nil
	})
	defer patchUpdate.Unpatch()
	defer patchRead.Unpatch()
	defer patchUnmarshalJs.Unpatch()
	col.Update(0, map[string]interface{}{"test": "fail json"})

	if !strings.Contains(buf.String(), "Will not attempt to unindex document") {
		t.Error("Expected log")
	}
}
func TestUpdateBytesFunc(t *testing.T) {
	var part *data.Partition
	db, _ := OpenDB(tempDir)
	defer os.RemoveAll(tempDir)
	errMessage := "error read"
	col, _ := OpenCol(db, "test")
	patchRead := monkey.PatchInstanceMethod(reflect.TypeOf(part), "Read", func(_ *data.Partition, id int) ([]byte, error) {
		return nil, errors.New(errMessage)
	})
	defer patchRead.Unpatch()
	if col.UpdateBytesFunc(0, func(origDoc []byte) (newDoc []byte, err error) {
		return []byte{}, nil
	}).Error() != errMessage {
		t.Errorf("expected error message %s", errMessage)
	}
}
func TestUpdateBytesCallbackError(t *testing.T) {
	var part *data.Partition
	db, _ := OpenDB(tempDir)
	defer os.RemoveAll(tempDir)
	errMessage := "error update"
	col, _ := OpenCol(db, "test")
	patchRead := monkey.PatchInstanceMethod(reflect.TypeOf(part), "Read", func(_ *data.Partition, id int) ([]byte, error) {
		return nil, nil
	})
	defer patchRead.Unpatch()
	if col.UpdateBytesFunc(0, func(origDoc []byte) (newDoc []byte, err error) {
		return []byte{}, errors.New(errMessage)
	}).Error() != errMessage {
		t.Errorf("expected error message %s", errMessage)
	}
}
func TestUpdateBytesJsMarshalErr(t *testing.T) {
	var part *data.Partition
	db, _ := OpenDB(tempDir)
	defer os.RemoveAll(tempDir)
	errMessage := "error update"
	col, _ := OpenCol(db, "test")
	patchRead := monkey.PatchInstanceMethod(reflect.TypeOf(part), "Read", func(_ *data.Partition, id int) ([]byte, error) {
		return nil, nil
	})
	patchMarshal := monkey.Patch(json.Unmarshal, func(data []byte, v interface{}) error {
		return errors.New(errMessage)
	})
	defer patchRead.Unpatch()
	defer patchMarshal.Unpatch()
	if col.UpdateBytesFunc(0, func(origDoc []byte) (newDoc []byte, err error) {
		return []byte{}, nil
	}).Error() != errMessage {
		t.Errorf("expected error message %s", errMessage)
	}
}
func TestUpdateBytesPartUpdateErr(t *testing.T) {
	var part *data.Partition
	db, _ := OpenDB(tempDir)
	defer os.RemoveAll(tempDir)
	errMessage := "error update"
	col, _ := OpenCol(db, "test")
	patchRead := monkey.PatchInstanceMethod(reflect.TypeOf(part), "Read", func(_ *data.Partition, id int) ([]byte, error) {
		return nil, nil
	})
	patchMarshal := monkey.Patch(json.Unmarshal, func(data []byte, v interface{}) error {
		return nil
	})
	patchUpdate := monkey.PatchInstanceMethod(reflect.TypeOf(part), "Update", func(_ *data.Partition, id int, data []byte) (err error) {
		return errors.New(errMessage)
	})
	defer patchRead.Unpatch()
	defer patchMarshal.Unpatch()
	defer patchUpdate.Unpatch()
	if col.UpdateBytesFunc(0, func(origDoc []byte) (newDoc []byte, err error) {
		return []byte{}, nil
	}).Error() != errMessage {
		t.Errorf("expected error message %s", errMessage)
	}
}
func TestUpdateBytesFuncIsLog(t *testing.T) {
	var (
		part *data.Partition
		str  bytes.Buffer
	)
	log.SetOutput(&str)

	db, _ := OpenDB(tempDir)

	defer os.RemoveAll(tempDir)
	//errMessage := "error update"
	col, _ := OpenCol(db, "test")
	patchRead := monkey.PatchInstanceMethod(reflect.TypeOf(part), "Read", func(_ *data.Partition, id int) ([]byte, error) {
		return nil, nil
	})
	patchMarshal := monkey.Patch(json.Unmarshal, func(data []byte, v interface{}) error {
		v = nil
		return nil
	})
	patchUpdate := monkey.PatchInstanceMethod(reflect.TypeOf(part), "Update", func(_ *data.Partition, id int, data []byte) (err error) {
		return nil
	})

	patchUnlock := monkey.PatchInstanceMethod(reflect.TypeOf(part), "UnlockUpdate", func(_ *data.Partition, id int) {
		return
	})

	defer patchRead.Unpatch()
	defer patchUnlock.Unpatch()
	defer patchMarshal.Unpatch()
	defer patchUpdate.Unpatch()

	col.UpdateBytesFunc(0, func(origDoc []byte) (newDoc []byte, err error) {
		return []byte{}, nil
	})
	if !strings.Contains(str.String(), "Will not attempt to unindex document") {
		t.Error("Expected message log")
	}
}
func TestUpdateFuncDocNotExistError(t *testing.T) {
	db, _ := OpenDB(tempDir)
	defer os.RemoveAll(tempDir)
	col, _ := OpenCol(db, "test")
	err := col.UpdateFunc(0, func(origDoc map[string]interface{}) (newDoc map[string]interface{}, err error) {
		return nil, nil
	})

	if err.Error() != "Document `0` does not exist" {
		t.Error("Expected error document not exist")
	}
}
func TestUpdateFuncUnmarshalError(t *testing.T) {
	db, _ := OpenDB(tempDir)
	defer os.RemoveAll(tempDir)
	col, _ := OpenCol(db, "test")
	id, _ := col.Insert(map[string]interface{}{"test": "test"})

	errMessage := "Error json marshal"
	patchMarshal := monkey.Patch(json.Unmarshal, func(data []byte, v interface{}) error {
		return errors.New(errMessage)
	})
	defer patchMarshal.Unpatch()

	err := col.UpdateFunc(id, func(origDoc map[string]interface{}) (newDoc map[string]interface{}, err error) {
		return nil, nil
	})

	if err.Error() != errMessage {
		t.Error("Expected error json marshaling")
	}
}
func TestUpdateFuncMarshalError(t *testing.T) {
	db, _ := OpenDB(tempDir)
	defer os.RemoveAll(tempDir)
	col, _ := OpenCol(db, "test")
	id, _ := col.Insert(map[string]interface{}{"test": "test"})

	errMessage := "Error json marshal"
	patchMarshal := monkey.Patch(json.Marshal, func(v interface{}) ([]byte, error) {
		return nil, errors.New(errMessage)
	})
	defer patchMarshal.Unpatch()

	err := col.UpdateFunc(id, func(origDoc map[string]interface{}) (newDoc map[string]interface{}, err error) {
		return nil, nil
	})

	if err.Error() != errMessage {
		t.Error("Expected error json marshaling")
	}
}
func TestUpdateFuncUpdateError(t *testing.T) {
	db, _ := OpenDB(tempDir)
	defer os.RemoveAll(tempDir)
	col, _ := OpenCol(db, "test")
	id, _ := col.Insert(map[string]interface{}{"test": "test"})

	errMessage := "Error update"
	err := col.UpdateFunc(id, func(origDoc map[string]interface{}) (newDoc map[string]interface{}, err error) {
		return nil, errors.New(errMessage)
	})

	if err.Error() != errMessage {
		t.Errorf("Expected error: %s", errMessage)
	}
}
func TestUpdateFuncPartUpdateError(t *testing.T) {
	var part *data.Partition
	db, _ := OpenDB(tempDir)
	defer os.RemoveAll(tempDir)
	errMessage := "Error update"
	col, _ := OpenCol(db, "test")
	id, _ := col.Insert(map[string]interface{}{"test": "test"})
	patchUpdate := monkey.PatchInstanceMethod(reflect.TypeOf(part), "Update", func(_ *data.Partition, id int, data []byte) (err error) {
		return nil
	})
	defer patchUpdate.Unpatch()
	err := col.UpdateFunc(id, func(origDoc map[string]interface{}) (newDoc map[string]interface{}, err error) {
		return nil, errors.New(errMessage)
	})

	if err.Error() != errMessage {
		t.Errorf("Expected error: %s", errMessage)
	}
}
func TestDeleteError(t *testing.T) {
	var part *data.Partition
	db, _ := OpenDB(tempDir)
	defer os.RemoveAll(tempDir)
	errMessage := "Error delete"
	col, _ := OpenCol(db, "test")

	id, _ := col.Insert(map[string]interface{}{"test": "test"})
	patchUpdate := monkey.PatchInstanceMethod(reflect.TypeOf(part), "Delete", func(_ *data.Partition, id int) (err error) {
		return errors.New(errMessage)
	})
	defer patchUpdate.Unpatch()
	err := col.Delete(id)

	if err.Error() != errMessage {
		t.Errorf("Expected error: %s", errMessage)
	}
}
func TestDeleteMarshalJsError(t *testing.T) {
	var (
		part *data.Partition
		str  bytes.Buffer
	)
	log.SetOutput(&str)
	db, _ := OpenDB(tempDir)
	defer os.RemoveAll(tempDir)
	errMessage := "Error json marshal"
	col, _ := OpenCol(db, "test")

	id, _ := col.Insert(map[string]interface{}{"test": "test"})
	patchUpdate := monkey.PatchInstanceMethod(reflect.TypeOf(part), "Delete", func(_ *data.Partition, id int) (err error) {
		return nil
	})
	patchMarshal := monkey.Patch(json.Unmarshal, func(data []byte, v interface{}) error {
		return errors.New(errMessage)
	})
	defer patchMarshal.Unpatch()
	defer patchUpdate.Unpatch()
	col.Delete(id)

	if !strings.Contains(str.String(), "Will not attempt to unindex document") {
		t.Error("Expected error: message log")
	}
}
