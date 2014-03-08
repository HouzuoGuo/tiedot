/* Query processor for embedded and HTTP API V2. */
package db

import (
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/chunk"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"github.com/HouzuoGuo/tiedot/uid"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
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
	resultMutex := &sync.Mutex{}
	collectIDs := func(id uint64, _ map[string]interface{}) bool {
		resultMutex.Lock()
		(*result)[id] = struct{}{}
		resultMutex.Unlock()
		return true
	}
	src.ForAll(collectIDs)
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
	lookupValueHash := chunk.StrHash(lookupStrValue)
	scanPath := strings.Join(vecPath, INDEX_PATH_SEP)

	// Is it PK index?
	if path == uid.PK_NAME {
		// Convert lookup string value (which is the Persistent ID) to integer and put it into result
		strint, err := strconv.ParseUint(lookupStrValue, 10, 64)
		if err != nil {
			return err
		}
		(*result)[strint] = struct{}{}
		return nil
	}

	// It might be a secondary index
	if secIndex, ok := src.SecIndexes[scanPath]; ok {
		num := lookupValueHash % src.NumChunksI64
		ht := secIndex[num]
		ht.Mutex.RLock()
		_, vals := ht.Get(lookupValueHash, intLimit)
		ht.Mutex.RUnlock()
		for _, v := range vals {
			(*result)[v] = struct{}{}
		}
		return
	}
	// Neither PK or secondary index...
	return errors.New(fmt.Sprintf("Please index %v and retry query %v", scanPath, expr))
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
	intLimit := 0
	if limit, hasLimit := expr["limit"]; hasLimit {
		if floatLimit, ok := limit.(float64); ok {
			intLimit = int(floatLimit)
		} else {
			return errors.New(fmt.Sprintf("Expecting `limit` as a number, but %v given", limit))
		}
	}
	if vecPath[0] == uid.PK_NAME {
		return errors.New("@id is the primary index, path existence test on @id is meaningless")
	}
	jointPath := strings.Join(vecPath, INDEX_PATH_SEP)
	if secIndex, ok := src.SecIndexes[jointPath]; ok {
		counter := 0
		for _, ht := range secIndex {
			_, vals := ht.GetAll(uint64(intLimit - counter))
			for _, v := range vals {
				(*result)[v] = struct{}{}
				counter++
				if counter == intLimit {
					return nil
				}
			}
		}
		return nil
	} else {
		return errors.New(fmt.Sprintf("Please index %v and retry query %v", vecPath, expr))
	}
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
	if vecPath[0] == uid.PK_NAME {
		return errors.New("@id is the primary index, integer range scan on @id is meaningless")
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
	if _, indexScan := src.SecIndexes[htPath]; indexScan {
		// Use index scan if it is available
		if from < to {
			// Forward scan - from low value to high value
			for lookupValue := from; lookupValue <= to; lookupValue++ {
				lookupStrValue := fmt.Sprint(lookupValue)
				hashValue := chunk.StrHash(lookupStrValue)
				_, vals := src.HashScan(htPath, hashValue, uint64(intLimit))
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
				_, vals := src.HashScan(htPath, hashValue, uint64(intLimit))
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
		return errors.New(fmt.Sprintf("Please index %v and retry query %v", vecPath, expr))
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
	resultMutex := &sync.Mutex{}
	docMatcher := func(id uint64, doc map[string]interface{}) bool {
		// Get inside the document and find value match
		for _, v := range GetIn(doc, vecPath) {
			if validRegexp.MatchString(fmt.Sprint(v)) {
				if intLimit > 0 && counter == intLimit {
					return false
				}
				resultMutex.Lock()
				(*result)[id] = struct{}{}
				counter += 1
				resultMutex.Unlock()
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
	case []interface{}: // [sub query 1, sub query 2, etc]
		return EvalUnion(expr, src, result)
	case string:
		if expr == "all" { // Put all IDs into result
			return EvalAllIDs(src, result)
		} else {
			// Might be single document number
			docID, err := strconv.ParseUint(expr, 10, 64)
			if err != nil {
				return errors.New(fmt.Sprintf("%s is not a document PK ID", expr))
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
		} else if lookupRegexp, lookup := expr["re"]; lookup { // find documents using regular expression
			return RegexpLookup(lookupRegexp, expr, src, result)
		} else {
			return errors.New(fmt.Sprintf("Query %v does not contain any operation (lookup/union/etc)", expr))
		}
	}
	return nil
}

//Escape any control characters,  ", or \ in the string, and wrap in double quotes
func escapeJSONString(s string) string {
	replacerTable := []string{
		`"`, `\"`, `\`, `\\`,
		"\u0000", "\\\u0000", "\u0001", "\\\u0001",
		"\u0002", "\\\u0002", "\u0003", "\\\u0003",
		"\u0004", "\\\u0004", "\u0005", "\\\u0005",
		"\u0006", "\\\u0006", "\u0007", "\\\u0007",
		"\u0008", "\\\u0008", "\u0009", "\\\u0009",
		"\u000A", "\\\u000A", "\u000B", "\\\u000B",
		"\u000C", "\\\u000C", "\u000D", "\\\u000D",
		"\u000E", "\\\u000E", "\u000F", "\\\u000F",
		"\u0010", "\\\u0010", "\u0011", "\\\u0011",
		"\u0012", "\\\u0012", "\u0013", "\\\u0013",
		"\u0014", "\\\u0014", "\u0015", "\\\u0015",
		"\u0016", "\\\u0016", "\u0017", "\\\u0017",
		"\u0018", "\\\u0018", "\u0019", "\\\u0019",
		"\u001A", "\\\u001A", "\u001B", "\\\u001B",
		"\u001C", "\\\u001C", "\u001D", "\\\u001D",
		"\u001E", "\\\u001E", "\u001F", "\\\u001F",
	}
	jsonEscaper := strings.NewReplacer(replacerTable...)
	return `"` + jsonEscaper.Replace(s) + `"`
}

// Detect ? characters outside of an embedded string, and replace them with
// the appropriate parameter, which will be encoded as a string.
// Does not verify that the resulting JSON is valid, but should not allow
// injecting parameters that change the structure of the source JSON (a la SQL
// injection).  Use this to derive final query strings from any user-supplied input.
//
// ParameterizeJSON(`{"eq": "New Go release", "in": [?]}`, `"thing1","thing2"`)
// -> {"eq": "New Go release", "in": ["\"thing1\",\"thing2\""]}
func ParameterizeJSON(q string, params ...string) string {
	const backslash_rune = 92
	const dbl_quote_rune = 34
	const question_rune = 63

	//Track the state of the cursor
	backslash := false
	in_quotes := false
	param_positions := make([]int, 0)
	for i, c := range q {
		if in_quotes {
			if backslash {
				//Ignore the character after a backslash (ie, an escape)
				//Technically we might need to ignore up to 5 for unicode literals
				//But those are themselves valid characters for a string & shouldn't
				//embed anything that changes escaping of subsequent characters
				//Be careful though if you plan on writing invalid JSON in the first
				//place - if you do something like "\u43"?" you might be able to
				//cause a syntax error in a different place
				backslash = false
			} else if c == backslash_rune {
				//Start escaping if we haven't already
				backslash = true
			} else if c == dbl_quote_rune {
				//If we're in a string and not escaping, a quote ends the present string
				in_quotes = false
			}
		} else if c == question_rune {
			//Not in quotes (ie, outside a string) a naked question rune is interpolated
			param_positions = append(param_positions, i)
		} else if c == dbl_quote_rune {
			//Not in quotes, a double quote starts a string
			in_quotes = true
		}
	}

	//Loop again, this time splitting at those positions and inserting the
	//appropriate param
	param_ct := 0
	accumulator := make([]byte, 0, len(q))
	for i, c := range q {
		if len(param_positions) == 0 || i != param_positions[0] {
			//Accumulate from the original into the output buffer
			buf := make([]byte, 8)
			n := utf8.EncodeRune(buf, c)
			buf = buf[:n]
			accumulator = append(accumulator, buf...)
		} else {
			//Interpolate
			interpolate_val := escapeJSONString(params[param_ct])
			//Append it to the output buffer
			accumulator = append(accumulator, []byte(interpolate_val)...)
			//Continue
			param_ct += 1
			param_positions = param_positions[1:]
		}
	}
	return string(accumulator)
}
