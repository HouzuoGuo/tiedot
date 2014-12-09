// Binary protocol over IPC - query processor.
package binprot

import (
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/dberr"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"strconv"
	"strings"
)

type query struct {
	colName, query string
	colID          int32
	colIDBytes     []byte
	result         *map[uint64]struct{}
}

// Run a query (deserialized from JSON) on the specified collection, store result document IDs inside the keys of the map.
func (client *BinProtClient) EvalQuery(query interface{}, colName string, result *map[uint64]struct{}) (err error) {
	client.opLock.Lock()
	colID, colIDBytes, err := client.colName2IDBytes(colName)
	if err != nil {
		client.opLock.Unlock()
		return
	}
	err = &query{
		colName:    colName,
		colID:      colID,
		colIDBytes: colIDBytes,
		result:     result}.eval(query)
	client.opLock.Unlock()
	return
}

// The main entry point of recursive query processing.
func (q *query) eval(op interface{}) (err error) {
	switch expr := op.(type) {
	case []interface{}: // [sub query 1, sub query 2, etc]
		return q.union(expr)
	case string:
		if expr == "all" {
			return q.allIDs()
		} else {
			// Might be single document number
			docID, err := strconv.ParseUint(expr, 10, 64)
			if err != nil {
				return dberr.Make(dberr.ErrorExpectingInt, "Single Document ID", docID)
			}
			(*q.result)[docID] = struct{}{}
		}
	case map[string]interface{}:
		if lookupValue, lookup := expr["eq"]; lookup { // eq - lookup
			return q.lookup(lookupValue, expr)
		} else if hasPath, exist := expr["has"]; exist { // has - path existence test
			return q.pathExists(hasPath, expr)
		} else if subExprs, intersect := expr["n"]; intersect { // n - intersection
			return q.intersect(subExprs)
		} else if subExprs, complement := expr["c"]; complement { // c - complement
			return q.complement(subExprs)
		} else if intFrom, htRange := expr["int-from"]; htRange { // int-from, int-to - integer range query
			return q.intRange(intFrom, expr)
		} else if intFrom, htRange := expr["int from"]; htRange { // "int from, "int to" - integer range query - same as above, just without dash
			return q.intRange(intFrom, expr)
		} else {
			return errors.New(fmt.Sprintf("Query %v does not contain any operation (lookup/union/etc)", expr))
		}
	}
	return nil
}

// Calculate union of sub-query results.
func (q *query) union(exprs []interface{}) (err error) {
	for _, subExpr := range exprs {
		if err = q.eval(subExpr); err != nil {
			return
		}
	}
	return
}

// Put all document IDs into result.
func (q *query) allIDs() (err error) {
	src.forEachDoc(func(id uint64, _ []byte) bool {
		(*result)[id] = struct{}{}
		return true
	})
	return
}

// Value equity check ("attribute == value") using hash lookup.
func (q *query) lookup(lookupValue interface{}, expr map[string]interface{}) (err error) {
	// Figure out lookup path - JSON array "in"
	path, hasPath := expr["in"]
	if !hasPath {
		return errors.New("Missing lookup path `in`")
	}
	vecPath := make([]string, 0)
	if vecPathInterface, ok := path.([]interface{}); ok {
		for _, v := range vecPathInterface {
			vecPath = append(vecPath, fmt.Sprint(v))
		}
	} else {
		return errors.New(fmt.Sprintf("Expecting vector lookup path `in`, but %v given", path))
	}
	// Figure out result number limit
	intLimit := uint64(0)
	if limit, hasLimit := expr["limit"]; hasLimit {
		if floatLimit, ok := limit.(float64); ok {
			intLimit = uint64(floatLimit)
		} else if _, ok := limit.(int); ok {
			intLimit = uint64(limit.(int))
		} else {
			return dberr.Make(dberr.ErrorExpectingInt, "limit", limit)
		}
	}
	lookupStrValue := fmt.Sprint(lookupValue) // the value to look for
	lookupValueHash := db.StrHashStrHash(lookupStrValue)
	scanPath := strings.Join(vecPath, db.INDEX_PATH_SEP)
	if _, indexed := src.indexPaths[scanPath]; !indexed {
		return dberr.Make(dberr.ErrorNeedIndex, scanPath, expr)
	}
	ht := src.hts[scanPath]
	vals := ht.Get(lookupValueHash, intLimit)
	for _, match := range vals {
		// Filter result to avoid hash collision
		if doc, err := src.read(match); err == nil {
			for _, v := range GetIn(doc, vecPath) {
				if fmt.Sprint(v) == lookupStrValue {
					(*result)[match] = struct{}{}
				}
			}
		}
	}
	return
}

// Value existence check (value != nil) using hash lookup.
func (q *query) pathExists(hasPath interface{}, expr map[string]interface{}) (err error) {
	// Figure out the path
	vecPath := make([]string, 0)
	if vecPathInterface, ok := hasPath.([]interface{}); ok {
		for _, v := range vecPathInterface {
			vecPath = append(vecPath, fmt.Sprint(v))
		}
	} else {
		return errors.New(fmt.Sprintf("Expecting vector path, but %v given", hasPath))
	}
	// Figure out result number limit
	intLimit := uint64(0)
	if limit, hasLimit := expr["limit"]; hasLimit {
		if floatLimit, ok := limit.(float64); ok {
			intLimit = uint64(floatLimit)
		} else if _, ok := limit.(int); ok {
			intLimit = uint64(limit.(int))
		} else {
			return dberr.Make(dberr.ErrorExpectingInt, "limit", limit)
		}
	}
	jointPath := strings.Join(vecPath, db.INDEX_PATH_SEP)
	if _, indexed := src.indexPaths[jointPath]; !indexed {
		return dberr.Make(dberr.ErrorNeedIndex, vecPath, expr)
	}
	counter := uint64(0)
	partDiv := src.approxDocCount() / 4000 // collect approx. 4k document IDs in each iteration
	if partDiv == 0 {
		partDiv++
	}
	ht := src.hts[jointPath]
	for i := uint64(0); i < partDiv; i++ {
		_, ids := ht.GetPartition(i, partDiv)
		for _, id := range ids {
			(*result)[id] = struct{}{}
			counter++
			if counter == intLimit {
				return nil
			}
		}
	}
	return nil
}

// Calculate intersection of sub-query results.
func (q *query) intersect(subExprs interface{}) (err error) {
	myResult := make(map[uint64]struct{})
	if subExprVecs, ok := subExprs.([]interface{}); ok {
		first := true
		for _, subExpr := range subExprVecs {
			subResult := make(map[uint64]struct{})
			intersection := make(map[uint64]struct{})
			if err = q.eval(subExpr, src, &subResult); err != nil {
				return
			}
			if first {
				myResult = subResult
				first = false
			} else {
				for k, _ := range subResult {
					if _, inBoth := myResult[k]; inBoth {
						intersection[k] = struct{}{}
					}
				}
				myResult = intersection
			}
		}
		for docID := range myResult {
			(*result)[docID] = struct{}{}
		}
	} else {
		return dberr.Make(dberr.ErrorExpectingSubQuery, subExprs)
	}
	return
}

// Calculate complement of sub-query results.
func (q *query) complement(subExprs interface{}) (err error) {
	myResult := make(map[uint64]struct{})
	if subExprVecs, ok := subExprs.([]interface{}); ok {
		for _, subExpr := range subExprVecs {
			subResult := make(map[uint64]struct{})
			complement := make(map[uint64]struct{})
			if err = q.eval(subExpr, src, &subResult); err != nil {
				return
			}
			for k, _ := range subResult {
				if _, inBoth := myResult[k]; !inBoth {
					complement[k] = struct{}{}
				}
			}
			for k, _ := range myResult {
				if _, inBoth := subResult[k]; !inBoth {
					complement[k] = struct{}{}
				}
			}
			myResult = complement
		}
		for docID := range myResult {
			(*result)[docID] = struct{}{}
		}
	} else {
		return dberr.Make(dberr.ErrorExpectingSubQuery, subExprs)
	}
	return
}

func (q *query) hashScan(idxName string, key, limit uint64) []uint64 {
	return col.hts[idxName].Get(key, limit)
}

// Look for indexed integer values within the specified integer range.
func (q *query) intRange(intFrom interface{}, expr map[string]interface{}) (err error) {
	path, hasPath := expr["in"]
	if !hasPath {
		return errors.New("Missing path `in`")
	}
	// Figure out the path
	vecPath := make([]string, 0)
	if vecPathInterface, ok := path.([]interface{}); ok {
		for _, v := range vecPathInterface {
			vecPath = append(vecPath, fmt.Sprint(v))
		}
	} else {
		return errors.New(fmt.Sprintf("Expecting vector path `in`, but %v given", path))
	}
	// Figure out result number limit
	intLimit := uint64(0)
	if limit, hasLimit := expr["limit"]; hasLimit {
		if floatLimit, ok := limit.(float64); ok {
			intLimit = uint64(floatLimit)
		} else if _, ok := limit.(int); ok {
			intLimit = uint64(limit.(int))
		} else {
			return dberr.Make(dberr.ErrorExpectingInt, limit)
		}
	}
	// Figure out the range ("from" value & "to" value)
	from, to := int(0), int(0)
	if floatFrom, ok := intFrom.(float64); ok {
		from = int(floatFrom)
	} else if _, ok := intFrom.(int); ok {
		from = intFrom.(int)
	} else {
		return dberr.Make(dberr.ErrorExpectingInt, "int-from", from)
	}
	if intTo, ok := expr["int-to"]; ok {
		if floatTo, ok := intTo.(float64); ok {
			to = int(floatTo)
		} else if _, ok := intTo.(int); ok {
			to = intTo.(int)
		} else {
			return dberr.Make(dberr.ErrorExpectingInt, "int-to", to)
		}
	} else if intTo, ok := expr["int to"]; ok {
		if floatTo, ok := intTo.(float64); ok {
			to = int(floatTo)
		} else if _, ok := intTo.(int); ok {
			to = intTo.(int)
		} else {
			return dberr.Make(dberr.ErrorExpectingInt, "int to", to)
		}
	} else {
		return dberr.Make(dberr.ErrorMissing, "int-to")
	}
	if to > from && to-from > 1000 || from > to && from-to > 1000 {
		tdlog.CritNoRepeat("Query %v involves index lookup on more than 1000 values, which can be very inefficient", expr)
	}
	counter := uint64(0) // Number of results already collected
	htPath := strings.Join(vecPath, ",")
	if _, indexScan := src.indexPaths[htPath]; !indexScan {
		return dberr.Make(dberr.ErrorNeedIndex, vecPath, expr)
	}
	if from < to {
		// Forward scan - from low value to high value
		for lookupValue := from; lookupValue <= to; lookupValue++ {
			hashValue := StrHash(strconv.Itoa(lookupValue))
			vals := src.hashScan(htPath, hashValue, intLimit)
			for _, docID := range vals {
				if intLimit > 0 && counter == intLimit {
					break
				}
				counter += 1
				(*result)[docID] = struct{}{}
			}
		}
	} else {
		// Backward scan - from high value to low value
		for lookupValue := from; lookupValue >= to; lookupValue-- {
			hashValue := StrHash(strconv.Itoa(lookupValue))
			vals := src.hashScan(htPath, hashValue, intLimit)
			for _, docID := range vals {
				if intLimit > 0 && counter == intLimit {
					break
				}
				counter += 1
				(*result)[docID] = struct{}{}
			}
		}
	}
	return
}

// TODO: How to bring back regex matcher?
// TODO: How to bring back JSON parameterized query?
