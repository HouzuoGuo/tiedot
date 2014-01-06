/* Query processor for embedded and HTTP API V2. */
package db

import (
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/chunk"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"regexp"
	"strings"
)

// Calculate union of sub-query results.
func EvalUnion(exprs []interface{}, src *Col, result *map[uint64]struct{}) (err error) {
	for _, subExpr := range exprs {
		// Evaluate all sub-queries - they will put their result into the result map
		if err = EvalQuery(subExpr, src, result); err != nil {
			return
		}
	}
	return
}

// Put all document IDs into result.
func EvalAllIDs(src *Col, result *map[uint64]struct{}) (err error) {
	collectIDs := func(id uint64, _ interface{}) bool {
		(*result)[id] = struct{}{}
		return true
	}
	numChunks := src.NumChunks
	for i := uint64(0); i < numChunks; i++ {
		lock := src.ChunkMutexes[i]
		lock.RLock()
		src.ForAll(collectIDs)
		lock.RUnlock()
	}
	return
}

// Execute value equity check ("attribute == value") using hash lookup or collection scan.
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
		} else {
			return errors.New(fmt.Sprintf("Expecting `limit` as a number, but %v given", limit))
		}
	}
	lookupStrValue := fmt.Sprint(lookupValue) // the value to match
	htPath := strings.Join(vecPath, ",")
	if _, indexScan := src.Chunks[0].Path2HT[htPath]; indexScan {
		// If index is available, do index scan
		// Hash collision detection function
		collisionDetection := func(k, v uint64) bool {
			var doc interface{}
			if src.Read(v, &doc) != nil {
				return false
			}
			// Actually get inside the document and match the value
			for _, v := range chunk.GetIn(doc, vecPath) {
				if fmt.Sprint(v) == lookupStrValue {
					return true
				}
			}
			return false
		}
		// Do hash scan
		_, scanResult := src.HashScan(htPath, chunk.StrHash(lookupStrValue), intLimit, collisionDetection)
		for _, docID := range scanResult {
			(*result)[docID] = struct{}{}
		}
	} else {
		// Do collection scan, when index is not available
		tdlog.Printf("Query %v is a collection scan, which may be inefficient", expr)
		counter := uint64(0)
		docMatcher := func(id uint64, doc interface{}) bool {
			// Get inside each document and find match
			for _, v := range chunk.GetIn(doc, vecPath) {
				if fmt.Sprint(v) == lookupStrValue {
					if intLimit > 0 && counter == intLimit {
						return false
					}
					(*result)[id] = struct{}{}
					counter += 1
				}
			}
			return true
		}
		src.ForAll(docMatcher)
	}
	return
}

// Execute value existence check.
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
		} else {
			return errors.New(fmt.Sprintf("Expecting `limit` as a number, but %v given", limit))
		}
	}

	// Get inside each document to find match
	counter := uint64(0)
	matchDocFunc := func(id uint64, doc interface{}) bool {
		vals := chunk.GetIn(doc, vecPath)
		if !(vals == nil || len(vals) == 1 && vals[0] == nil) {
			if intLimit > 0 && counter == intLimit {
				return false
			}
			(*result)[id] = struct{}{}
			counter += 1
		}
		return true
	}
	src.ForAll(matchDocFunc)
	return
}

// Calculate intersection of sub query results.
func Intersect(subExprs interface{}, src *Col, result *map[uint64]struct{}) (err error) {
	if subExprVecs, ok := subExprs.([]interface{}); ok {
		first := true
		for _, subExpr := range subExprVecs {
			subResult := make(map[uint64]struct{})
			intersection := make(map[uint64]struct{})
			if err = EvalQuery(subExpr, src, &subResult); err != nil {
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
func Complement(subExprs interface{}, src *Col, result *map[uint64]struct{}) (err error) {
	if subExprVecs, ok := subExprs.([]interface{}); ok {
		for _, subExpr := range subExprVecs {
			subResult := make(map[uint64]struct{})
			complement := make(map[uint64]struct{})
			if err = EvalQuery(subExpr, src, &subResult); err != nil {
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
	intLimit := int(0)
	if limit, hasLimit := expr["limit"]; hasLimit {
		if floatLimit, ok := limit.(float64); ok {
			intLimit = int(floatLimit)
		} else {
			return errors.New(fmt.Sprintf("Expecting `limit` as a number, but %v given", limit))
		}
	}
	// Figure out the range ("from" value & "to" value)
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
	} else if intTo, ok := expr["int to"]; ok {
		if floatTo, ok := intTo.(float64); ok {
			to = int(floatTo)
		} else {
			return errors.New(fmt.Sprintf("Expecting `int-to` as an integer, but %v given", to))
		}
	} else {
		return errors.New(fmt.Sprintf("Missing `int-to`"))
	}
	if to > from && to-from > 1000 || from > to && from-to > 1000 {
		tdlog.Printf("Query %v is an index lookup of more than 1000 values, which may be inefficient", expr)
	}
	counter := int(0) // Number of results already collected
	htPath := strings.Join(vecPath, ",")
	if _, indexScan := src.Chunks[0].Path2HT[htPath]; indexScan {
		// Use index scan if it is available
		if from < to {
			// Forward scan - from low value to high value
			for lookupValue := from; lookupValue <= to; lookupValue++ {
				lookupStrValue := fmt.Sprint(lookupValue)
				hashValue := chunk.StrHash(lookupStrValue)
				// Hash collision detection function
				collisionDetection := func(k, v uint64) bool {
					var doc interface{}
					if src.Read(v, &doc) != nil {
						return false
					}
					for _, v := range chunk.GetIn(doc, vecPath) {
						if fmt.Sprint(v) == lookupStrValue {
							return true
						}
					}
					return false
				}
				_, vals := src.HashScan(htPath, hashValue, uint64(intLimit), collisionDetection)
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
				lookupStrValue := fmt.Sprint(lookupValue)
				hashValue := chunk.StrHash(lookupStrValue)
				collisionDetection := func(k, v uint64) bool {
					var doc interface{}
					if src.Read(v, &doc) != nil {
						return false
					}
					for _, v := range chunk.GetIn(doc, vecPath) {
						if fmt.Sprint(v) == lookupStrValue {
							return true
						}
					}
					return false
				}
				_, vals := src.HashScan(htPath, hashValue, uint64(intLimit), collisionDetection)
				for _, docID := range vals {
					if intLimit > 0 && counter == intLimit {
						break
					}
					counter += 1
					(*result)[docID] = struct{}{}
				}
			}
		}
	} else {
		// Fall back to collection scan, when index is not available
		tdlog.Printf("Query %v is a collection scan which can be *very* inefficient, also query \"limit\" and reverse range support is unavailable!", expr)
		// Reversed range cannot be supported, sorry
		if to < from {
			tmp := from
			from = to
			to = tmp
		}
		counter := int(0)
		docMatcher := func(id uint64, doc interface{}) bool {
			for _, v := range chunk.GetIn(doc, vecPath) {
				if floatV, ok := v.(float64); ok {
					if intV := int(floatV); intV <= to && intV >= from {
						if intLimit > 0 && counter == intLimit {
							return false
						}
						(*result)[id] = struct{}{}
						counter += 1
					}
				}
			}
			return true
		}
		src.ForAll(docMatcher)
	}
	return
}

// Execute value match regexp using hash lookup or collection scan.
func RegexpLookup(lookupRegexp interface{}, expr map[string]interface{}, src *Col, result *map[uint64]struct{}) (err error) {
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
		} else {
			return errors.New(fmt.Sprintf("Expecting `limit` as a number, but %v given", limit))
		}
	}
	regexpStrValue := fmt.Sprint(lookupRegexp)
	validRegexp := regexp.MustCompile(regexpStrValue)
	// Do collection scan
	counter := uint64(0)
	docMatcher := func(id uint64, doc interface{}) bool {
		// Get inside the document and find value match
		for _, v := range chunk.GetIn(doc, vecPath) {
			if validRegexp.MatchString(fmt.Sprint(v)) {
				if intLimit > 0 && counter == intLimit {
					return false
				}
				(*result)[id] = struct{}{}
				counter += 1
			}
		}
		return true
	}
	src.ForAll(docMatcher)

	return
}

// Main entrance to query processor - evaluate a query and put result into result map (as map keys).
func EvalQuery(q interface{}, src *Col, result *map[uint64]struct{}) (err error) {
	switch expr := q.(type) {
	case float64: // Single document number
		(*result)[uint64(expr)] = struct{}{}
	case []interface{}: // [sub query 1, sub query 2, etc]
		return EvalUnion(expr, src, result)
	case string:
		if expr == "all" { // Put all IDs into result
			return EvalAllIDs(src, result)
		} else {
			return errors.New(fmt.Sprintf("Do not know what %v means, did you mean 'all' (getting all document IDs)?", expr))
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
		} else if lookupRegexp, lookup := expr["re"]; lookup { // find documents using regular expression
			return RegexpLookup(lookupRegexp, expr, src, result)
		} else {
			return errors.New(fmt.Sprintf("Query %v does not contain any operation (lookup/union/etc)", expr))
		}
	}
	return nil
}
