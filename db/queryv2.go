/* Query processor for embedded and HTTP API V2. */
package db

import (
	"errors"
	"fmt"
	"log"
	"strings"
)

// Calculate union of sub-query results.
func V2EvalUnion(exprs []interface{}, src *Col, result *map[uint64]struct{}) (err error) {
	for _, subExpr := range exprs {
		// simply evaluate all sub-queries, they will put their results into the result map
		if err = EvalQueryV2(subExpr, src, result); err != nil {
			return
		}
	}
	return
}

// Put all document IDs into result.
func V2EvalAllIDs(src *Col, result *map[uint64]struct{}) (err error) {
	collectIDs := func(id uint64, _ []byte) bool {
		(*result)[id] = struct{}{}
		return true
	}
	src.Data.ForAll(collectIDs)
	return
}

// Execute value equity check ("attribute == value") using hash lookup or collection scan.
func V2Lookup(lookupValue interface{}, expr map[string]interface{}, src *Col, result *map[uint64]struct{}) (err error) {
	// figure out lookup path - JSON array "in"
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
	// figure out result number limit
	intLimit := uint64(0)
	if limit, hasLimit := expr["limit"]; hasLimit {
		if floatLimit, ok := limit.(float64); ok {
			intLimit = uint64(floatLimit)
		} else {
			return errors.New(fmt.Sprintf("Expecting `limit` as a number, but %v given", limit))
		}
	}
	lookupStrValue := fmt.Sprint(lookupValue) // the value to match
	if ht, indexScan := src.StrHT[strings.Join(vecPath, ",")]; indexScan {
		// do hash scan where possible
		collisionDetection := func(k, v uint64) bool {
			var doc interface{}
			if src.Read(v, &doc) != nil {
				return false
			}
			// make sure that hash table match is not a collision case
			for _, v := range GetIn(doc, vecPath) {
				if fmt.Sprint(v) == lookupStrValue {
					return true
				}
			}
			return false
		}
		hashValue := StrHash(lookupStrValue)
		// do hash scan
		_, scanResult := ht.Get(hashValue, intLimit, collisionDetection)
		for _, docID := range scanResult {
			(*result)[docID] = struct{}{}
		}
	} else {
		// index is not available, do collection scan instead
		log.Printf("Query %v is a collection scan, which may be inefficient", expr)
		counter := uint64(0)
		docMatcher := func(id uint64, doc interface{}) bool {
			// get inside the document and find value match
			for _, v := range GetIn(doc, vecPath) {
				if fmt.Sprint(v) == lookupStrValue {
					(*result)[id] = struct{}{}
					counter += 1
					return counter != intLimit
				}
			}
			return true
		}
		src.ForAll(docMatcher)
	}
	return
}

// Execute value existence check.
func V2PathExistence(hasPath interface{}, expr map[string]interface{}, src *Col, result *map[uint64]struct{}) (err error) {
	// figure out the path
	vecPath := make([]string, 0)
	if vecPathInterface, ok := hasPath.([]interface{}); ok {
		for _, v := range vecPathInterface {
			vecPath = append(vecPath, fmt.Sprint(v))
		}
	} else {
		return errors.New(fmt.Sprintf("Expecting vector path, but %v given", hasPath))
	}
	// figure out result number limit
	intLimit := uint64(0)
	if limit, hasLimit := expr["limit"]; hasLimit {
		if floatLimit, ok := limit.(float64); ok {
			intLimit = uint64(floatLimit)
		} else {
			return errors.New(fmt.Sprintf("Expecting `limit` as a number, but %v given", limit))
		}
	}
	// depends on the availability of index and size of collection, determine whether to do hash scan or collection scan
	if ht, indexScan := src.StrHT[strings.Join(vecPath, ",")]; indexScan && src.Data.File.Append >= 67108864 {
		// ht.GetAll is actually quite expensive (50-100 ops/sec), so it may not be necessary if data is smaller than 64MB
		_, vals := ht.GetAll(intLimit)
		for _, docID := range vals {
			(*result)[docID] = struct{}{}
		}
	} else {
		counter := uint64(0)
		matchDocFunc := func(id uint64, doc interface{}) bool {
			vals := GetIn(doc, vecPath)
			if !(vals == nil || len(vals) == 1 && vals[0] == nil) {
				(*result)[id] = struct{}{}
				counter += 1
				return counter != intLimit
			}
			return true
		}
		src.ForAll(matchDocFunc)
	}
	return
}

// Calculate intersection of sub query results.
func V2Intersect(subExprs interface{}, src *Col, result *map[uint64]struct{}) (err error) {
	if subExprVecs, ok := subExprs.([]interface{}); ok {
		first := true
		for _, subExpr := range subExprVecs {
			subResult := make(map[uint64]struct{})
			intersection := make(map[uint64]struct{})
			if err = EvalQueryV2(subExpr, src, &subResult); err != nil {
				return
			}
			if first {
				*result = subResult
				first = false
			} else {
				for k, _ := range subResult {
					if _, inBoth := (*result)[k]; inBoth {
						intersection[k] = struct{}{}
					}
				}
				*result = intersection
			}
		}
	} else {
		return errors.New(fmt.Sprintf("Expecting a vector of sub-queries, but %v given", subExprs))
	}
	return
}

// Calculate complement of sub query results.
func V2Complement(subExprs interface{}, src *Col, result *map[uint64]struct{}) (err error) {
	if subExprVecs, ok := subExprs.([]interface{}); ok {
		for _, subExpr := range subExprVecs {
			subResult := make(map[uint64]struct{})
			complement := make(map[uint64]struct{})
			if err = EvalQueryV2(subExpr, src, &subResult); err != nil {
				return
			}
			for k, _ := range subResult {
				if _, inBoth := (*result)[k]; !inBoth {
					complement[k] = struct{}{}
				}
			}
			for k, _ := range *result {
				if _, inBoth := subResult[k]; !inBoth {
					complement[k] = struct{}{}
				}
			}
			*result = complement
		}
	} else {
		return errors.New(fmt.Sprintf("Expecting a vector of sub-queries, but %v given", subExprs))
	}
	return
}

// Scan hash table or collection documents using an integer range.
func V2IntRange(intFrom interface{}, expr map[string]interface{}, src *Col, result *map[uint64]struct{}) (err error) {
	path, hasPath := expr["in"]
	if !hasPath {
		return errors.New("Missing path `in`")
	}
	// figure out the path
	vecPath := make([]string, 0)
	if vecPathInterface, ok := path.([]interface{}); ok {
		for _, v := range vecPathInterface {
			vecPath = append(vecPath, fmt.Sprint(v))
		}
	} else {
		return errors.New(fmt.Sprintf("Expecting vector path `in`, but %v given", path))
	}
	// figure out result number limit
	intLimit := int(0)
	if limit, hasLimit := expr["limit"]; hasLimit {
		if floatLimit, ok := limit.(float64); ok {
			intLimit = int(floatLimit)
		} else {
			return errors.New(fmt.Sprintf("Expecting `limit` as a number, but %v given", limit))
		}
	}
	// figure out the range ("from" value & "to" value)
	from, to := int(0), int(0)
	if floatFrom, ok := intFrom.(float64); ok {
		from = int(floatFrom)
	} else {
		return errors.New(fmt.Sprintf("Expecting `int-from` as an integer, but %v given", from))
	}
	if intTo, ok := expr["int-to"]; ok {
		if floatTo, ok := intTo.(float64); ok {
			to = int(floatTo)
		} else {
			return errors.New(fmt.Sprintf("Expecting `int-to` as an integer, but %v given", to))
		}
	} else {
		return errors.New(fmt.Sprintf("Missing `int-to`"))
	}
	if to > from && to-from > 1000 || from > to && from-to > 1000 {
		log.Printf("Query %v is an index lookup of more than 1000 values, which may be inefficient", expr)
	}
	counter := int(0) // number of results already collected
	if ht, indexScan := src.StrHT[strings.Join(vecPath, ",")]; indexScan {
		// if index is available, do index scan
		if from < to {
			// direction is forward
			for lookupValue := from; lookupValue <= to; lookupValue++ {
				lookupStrValue := fmt.Sprint(lookupValue)
				hashValue := StrHash(lookupStrValue)
				collisionDetection := func(k, v uint64) bool {
					var doc interface{}
					if src.Read(v, &doc) != nil {
						return false
					}
					// make sure that hash table match is not a collision case
					for _, v := range GetIn(doc, vecPath) {
						if fmt.Sprint(v) == lookupStrValue {
							return true
						}
					}
					return false
				}
				_, vals := ht.Get(hashValue, uint64(intLimit), collisionDetection)
				for _, docID := range vals {
					if intLimit != 0 && counter == intLimit {
						break
					}
					counter += 1
					(*result)[docID] = struct{}{}
				}
			}
		} else {
			// direction is backward
			for lookupValue := from; lookupValue >= to; lookupValue-- {
				lookupStrValue := fmt.Sprint(lookupValue)
				hashValue := StrHash(lookupStrValue)
				collisionDetection := func(k, v uint64) bool {
					var doc interface{}
					if src.Read(v, &doc) != nil {
						return false
					}
					// make sure that hash table match is not a collision case
					for _, v := range GetIn(doc, vecPath) {
						if fmt.Sprint(v) == lookupStrValue {
							return true
						}
					}
					return false
				}
				_, vals := ht.Get(hashValue, uint64(intLimit), collisionDetection)
				for _, docID := range vals {
					if intLimit != 0 && counter == intLimit {
						break
					}
					counter += 1
					(*result)[docID] = struct{}{}
				}
			}
		}
	} else {
		// do collection scan when index is not available
		log.Printf("Query %v is a collection scan which can be *very* inefficient, also query \"limit\" and reverse range support is unavailable!", expr)
		// reverse range is close to impossible in collection scan, sorry!
		if to < from {
			tmp := from
			from = to
			to = tmp
		}
		counter := int(0)
		docMatcher := func(id uint64, doc interface{}) bool {
			for _, v := range GetIn(doc, vecPath) {
				if floatV, ok := v.(float64); ok {
					if intV := int(floatV); intV <= to && intV >= from {
						(*result)[id] = struct{}{}
						counter += 1
						return counter != intLimit
					}
				}
			}
			return true
		}
		src.ForAll(docMatcher)
	}
	return
}

// Main entrance to query processor - evaluate a query and put result into result map (as map keys).
func EvalQueryV2(q interface{}, src *Col, result *map[uint64]struct{}) (err error) {
	switch expr := q.(type) {
	case float64:
		// single document number
		(*result)[uint64(expr)] = struct{}{}
	case []interface{}: // [sub query 1, sub query 2, etc]
		return V2EvalUnion(expr, src, result)
	case string:
		if expr == "all" { // put all IDs into result
			return V2EvalAllIDs(src, result)
		} else {
			return errors.New(fmt.Sprintf("Do not know what %v means, did you mean 'all' (getting all document IDs)?", expr))
		}
	case map[string]interface{}:
		if lookupValue, lookup := expr["eq"]; lookup { // eq - lookup
			return V2Lookup(lookupValue, expr, src, result)
		} else if hasPath, exist := expr["has"]; exist { // has - path existence test
			return V2PathExistence(hasPath, expr, src, result)
		} else if subExprs, intersect := expr["n"]; intersect { // n - intersection
			return V2Intersect(subExprs, src, result)
		} else if subExprs, complement := expr["c"]; complement { // c - complement
			return V2Complement(subExprs, src, result)
		} else if intFrom, htRange := expr["int-from"]; htRange { // int-from, int-to - integer range query
			return V2IntRange(intFrom, expr, src, result)
		} else {
			return errors.New(fmt.Sprintf("Query %v does not contain any operation (lookup/union/etc)", expr))
		}
	}
	return nil
}
