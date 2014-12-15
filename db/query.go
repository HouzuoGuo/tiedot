/* Query processor. */
package db

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/HouzuoGuo/tiedot/dberr"
	"github.com/HouzuoGuo/tiedot/tdlog"
)

// Calculate union of sub-query results.
func EvalUnion(exprs []interface{}, src *Col, result *map[uint64]struct{}) (err error) {
	for _, subExpr := range exprs {
		myResult := make(map[uint64]struct{})
		if err = evalQuery(subExpr, src, result); err != nil {
			return
		}
		for k := range myResult {
			(*result)[k] = struct{}{}
		}
	}
	return
}

// Put all document IDs into result.
func EvalAllIDs(src *Col, result *map[uint64]struct{}) (err error) {
	src.forEachDoc(func(id uint64, _ []byte) bool {
		(*result)[id] = struct{}{}
		return true
	})
	return
}

// Value equity check ("attribute == value") using hash lookup.
func Lookup(lookupValue interface{}, expr map[string]interface{}, src *Col, result *map[uint64]struct{}) (err error) {
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
	lookupValueHash := StrHash(lookupStrValue)
	scanPath := strings.Join(vecPath, INDEX_PATH_SEP)
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
func PathExistence(hasPath interface{}, expr map[string]interface{}, src *Col, result *map[uint64]struct{}) (err error) {
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
	jointPath := strings.Join(vecPath, INDEX_PATH_SEP)
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
func Intersect(subExprs interface{}, src *Col, result *map[uint64]struct{}) (err error) {
	myResult := make(map[uint64]struct{})
	if subExprVecs, ok := subExprs.([]interface{}); ok {
		first := true
		for _, subExpr := range subExprVecs {
			subResult := make(map[uint64]struct{})
			intersection := make(map[uint64]struct{})
			if err = evalQuery(subExpr, src, &subResult); err != nil {
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
func Complement(subExprs interface{}, src *Col, result *map[uint64]struct{}) (err error) {
	myResult := make(map[uint64]struct{})
	if subExprVecs, ok := subExprs.([]interface{}); ok {
		for _, subExpr := range subExprVecs {
			subResult := make(map[uint64]struct{})
			complement := make(map[uint64]struct{})
			if err = evalQuery(subExpr, src, &subResult); err != nil {
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

func (col *Col) hashScan(idxName string, key, limit uint64) []uint64 {
	return col.hts[idxName].Get(key, limit)
}

// Look for indexed integer values within the specified integer range.
func IntRange(intFrom interface{}, expr map[string]interface{}, src *Col, result *map[uint64]struct{}) (err error) {
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

func evalQuery(q interface{}, src *Col, result *map[uint64]struct{}) (err error) {
	switch expr := q.(type) {
	case []interface{}: // [sub query 1, sub query 2, etc]
		return EvalUnion(expr, src, result)
	case string:
		if expr == "all" {
			return EvalAllIDs(src, result)
		} else {
			// Might be single document number
			docID, err := strconv.ParseUint(expr, 10, 64)
			if err != nil {
				return dberr.Make(dberr.ErrorExpectingInt, "Single Document ID", docID)
			}
			(*result)[docID] = struct{}{}
		}
	case map[string]interface{}:
		if lookupValue, lookup := expr["eq"]; lookup { // eq - lookup
			return Lookup(lookupValue, expr, src, result)
		} else if hasPath, exist := expr["has"]; exist { // has - path existence test
			return PathExistence(hasPath, expr, src, result)
		} else if subExprs, intersect := expr["n"]; intersect { // n - intersection
			return Intersect(subExprs, src, result)
		} else if subExprs, complement := expr["c"]; complement { // c - complement
			return Complement(subExprs, src, result)
		} else if intFrom, htRange := expr["int-from"]; htRange { // int-from, int-to - integer range query
			return IntRange(intFrom, expr, src, result)
		} else if intFrom, htRange := expr["int from"]; htRange { // "int from, "int to" - integer range query - same as above, just without dash
			return IntRange(intFrom, expr, src, result)
		} else {
			return errors.New(fmt.Sprintf("Query %v does not contain any operation (lookup/union/etc)", expr))
		}
	}
	return nil
}

// Main entrance to query processor - evaluate a query and put result into result map (as map keys).
func EvalQuery(q interface{}, src *Col, result *map[uint64]struct{}) (err error) {
	src.db.lock.RLock()
	err = evalQuery(q, src, result)
	src.db.lock.RUnlock()
	return
}

// TODO: How to bring back regex matcher?
// TODO: How to bring back JSON parameterized query?
