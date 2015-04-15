// DB sharding via IPC using a binary protocol - query validation and processing.
package sharding

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/dberr"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math/rand"
	"strconv"
)

type Query struct {
	client         *RouterClient
	colName, query string
	colID          int32
	colIDBytes     []byte
}

// Run a query (deserialized from JSON) on the specified collection, store result document IDs inside the keys of the map.
func (client *RouterClient) EvalQuery(colName string, q interface{}, result *map[uint64]struct{}) (err error) {
	client.opLock.Lock()
	colID, colIDBytes, err := client.colName2IDBytes(colName)
	if err != nil {
		client.opLock.Unlock()
		return
	}
	// Place a lock on any rank as a signal of ongoing query operations
	lockedServer := rand.Intn(client.nProcs)
	if _, _, err = client.sendCmd(lockedServer, true, C_QUERY_PRE); err != nil {
		return
	}
	qStruct := &Query{
		client:     client,
		colName:    colName,
		colID:      colID,
		colIDBytes: colIDBytes}
	err = qStruct.eval(q, result)
	if _, _, errPost := client.sendCmd(lockedServer, true, C_QUERY_POST); errPost != nil {
		tdlog.Noticef("Client %d: failed to call QUERY_POST on server %d - %v, closing this client.", client.id, lockedServer, errPost)
		client.close()
	}
	client.opLock.Unlock()
	return
}

// Read and deserialize a document.
func (q *Query) readDeserializeDoc(docID uint64) (doc map[string]interface{}, err error) {
	rank, docIDBytes := q.client.docID2RankBytes(docID)
	_, resp, err := q.client.sendCmd(rank, true, C_DOC_READ, q.colIDBytes, docIDBytes)
	err = json.Unmarshal(resp[0], &doc)
	return
}

// Find an index or return error.
func (q *Query) getHTID(vecPath []string, originalQExpr interface{}) (htID int32, err error) {
	htID = q.client.dbo.GetIndexIDBySplitPath(q.colID, vecPath)
	if htID == -1 {
		err = dberr.New(dberr.ErrorNeedIndex, vecPath, originalQExpr)
	}
	return
}

// Recursively process the query operation.
func (q *Query) eval(queryExpr interface{}, result *map[uint64]struct{}) (err error) {
	switch expr := queryExpr.(type) {
	case []interface{}: // [sub query 1, sub query 2, etc]
		return q.union(expr, result)
	case string:
		if expr == "all" {
			return q.allIDs(result)
		} else {
			// Might be single document number
			docID, err := strconv.ParseUint(expr, 10, 64)
			if err != nil {
				return dberr.New(dberr.ErrorExpectingInt, "Single Document ID", docID)
			}
			(*result)[docID] = struct{}{}
		}
	case map[string]interface{}:
		if lookupValue, lookup := expr["eq"]; lookup { // eq - lookup
			return q.lookup(lookupValue, expr, result)
		} else if subExprs, intersect := expr["n"]; intersect { // n - intersection
			return q.intersect(subExprs, result)
		} else if subExprs, complement := expr["c"]; complement { // c - complement
			return q.complement(subExprs, result)
		} else if intFrom, htRange := expr["int-from"]; htRange { // int-from, int-to - integer range query
			return q.intRange(intFrom, expr, result)
		} else if intFrom, htRange := expr["int from"]; htRange { // "int from, "int to" - integer range query - same as above, just without dash
			return q.intRange(intFrom, expr, result)
		} else {
			return errors.New(fmt.Sprintf("Query %v does not contain any operation (lookup/union/etc)", expr))
		}
	}
	return nil
}

// Calculate union of sub-query results.
func (q *Query) union(exprs []interface{}, result *map[uint64]struct{}) (err error) {
	for _, subExpr := range exprs {
		myResult := make(map[uint64]struct{})
		if err = q.eval(subExpr, &myResult); err != nil {
			return
		}
		for k := range myResult {
			(*result)[k] = struct{}{}
		}
	}
	return
}

// Put all document IDs into result.
func (q *Query) allIDs(result *map[uint64]struct{}) (err error) {
	q.client.forEachDocBytes(q.colName, func(id uint64, _ []byte) bool {
		(*result)[id] = struct{}{}
		return true
	})
	return
}

// Value equity check ("attribute == value") using hash lookup.
func (q *Query) lookup(lookupValue interface{}, queryExpr map[string]interface{}, result *map[uint64]struct{}) (err error) {
	// Figure out lookup path - JSON array "in"
	path, hasPath := queryExpr["in"]
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
	if limit, hasLimit := queryExpr["limit"]; hasLimit {
		if floatLimit, ok := limit.(float64); ok {
			intLimit = uint64(floatLimit)
		} else if _, ok := limit.(int); ok {
			intLimit = uint64(limit.(int))
		} else {
			return dberr.New(dberr.ErrorExpectingInt, "limit", limit)
		}
	}
	lookupStrValue := fmt.Sprint(lookupValue) // the value to look for
	if htID, err := q.getHTID(vecPath, queryExpr); err != nil {
		return err
	} else if vals, err := q.client.hashLookup(htID, intLimit, lookupStrValue); err != nil {
		return err
	} else {
		for _, match := range vals {
			if doc, err := q.readDeserializeDoc(match); err == nil {
				for _, v := range ResolveDocAttr(doc, vecPath) {
					if fmt.Sprint(v) == lookupStrValue {
						(*result)[match] = struct{}{}
					}
				}
			}
		}
	}
	return
}

// Calculate intersection of sub-query results.
func (q *Query) intersect(subExprs interface{}, result *map[uint64]struct{}) (err error) {
	myResult := make(map[uint64]struct{})
	if subExprVecs, ok := subExprs.([]interface{}); ok {
		first := true
		for _, subExpr := range subExprVecs {
			subResult := make(map[uint64]struct{})
			intersection := make(map[uint64]struct{})
			if err = q.eval(subExpr, &subResult); err != nil {
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
		return dberr.New(dberr.ErrorExpectingSubQuery, subExprs)
	}
	return
}

// Calculate complement of sub-query results.
func (q *Query) complement(subExprs interface{}, result *map[uint64]struct{}) (err error) {
	myResult := make(map[uint64]struct{})
	if subExprVecs, ok := subExprs.([]interface{}); ok {
		for _, subExpr := range subExprVecs {
			subResult := make(map[uint64]struct{})
			complement := make(map[uint64]struct{})
			if err = q.eval(subExpr, &subResult); err != nil {
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
		return dberr.New(dberr.ErrorExpectingSubQuery, subExprs)
	}
	return
}

// Look for indexed integer values within the specified integer range.
func (q *Query) intRange(intFrom interface{}, queryExpr map[string]interface{}, result *map[uint64]struct{}) (err error) {
	path, hasPath := queryExpr["in"]
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
	if limit, hasLimit := queryExpr["limit"]; hasLimit {
		if floatLimit, ok := limit.(float64); ok {
			intLimit = uint64(floatLimit)
		} else if _, ok := limit.(int); ok {
			intLimit = uint64(limit.(int))
		} else {
			return dberr.New(dberr.ErrorExpectingInt, limit)
		}
	}
	// Figure out the range ("from" value & "to" value)
	from, to := int(0), int(0)
	if floatFrom, ok := intFrom.(float64); ok {
		from = int(floatFrom)
	} else if _, ok := intFrom.(int); ok {
		from = intFrom.(int)
	} else {
		return dberr.New(dberr.ErrorExpectingInt, "int-from", from)
	}
	if intTo, ok := queryExpr["int-to"]; ok {
		if floatTo, ok := intTo.(float64); ok {
			to = int(floatTo)
		} else if _, ok := intTo.(int); ok {
			to = intTo.(int)
		} else {
			return dberr.New(dberr.ErrorExpectingInt, "int-to", to)
		}
	} else if intTo, ok := queryExpr["int to"]; ok {
		if floatTo, ok := intTo.(float64); ok {
			to = int(floatTo)
		} else if _, ok := intTo.(int); ok {
			to = intTo.(int)
		} else {
			return dberr.New(dberr.ErrorExpectingInt, "int to", to)
		}
	} else {
		return dberr.New(dberr.ErrorMissing, "int-to")
	}
	if to > from && to-from > 1000 || from > to && from-to > 1000 {
		tdlog.CritNoRepeat("Query %v involves index lookup on more than 1000 values, which can be very inefficient", queryExpr)
	}
	counter := uint64(0) // Number of results already collected
	if from < to {
		// Forward scan - from low value to high value
		for lookupValue := from; lookupValue <= to; lookupValue++ {
			if htID, err := q.getHTID(vecPath, queryExpr); err != nil {
				return err
			} else if vals, err := q.client.hashLookup(htID, intLimit, strconv.Itoa(lookupValue)); err != nil {
				return err
			} else {
				for _, match := range vals {
					if intLimit > 0 && counter == intLimit {
						break
					}
					counter += 1
					(*result)[match] = struct{}{}
				}
			}
		}
	} else {
		// Backward scan - from high value to low value
		for lookupValue := from; lookupValue >= to; lookupValue-- {
			if htID, err := q.getHTID(vecPath, queryExpr); err != nil {
				return err
			} else if vals, err := q.client.hashLookup(htID, intLimit, strconv.Itoa(lookupValue)); err != nil {
				return err
			} else {
				for _, match := range vals {
					if intLimit > 0 && counter == intLimit {
						break
					}
					counter += 1
					(*result)[match] = struct{}{}
				}
			}
		}
	}
	return
}

// TODO: How to bring back regex matcher?
// TODO: How to bring back JSON parameterized query?
