package util

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"sort"
	"strings"
)

const (
	jsonIsUnknown = "bson.unknown"
	jsonIsBsonD   = "bson.d"
	jsonIsBsonA   = "bson.a"
)

func jsonIsBsonDOrBsonA(json string) string {
	json = strings.TrimSpace(json)
	if len(json) == 0 {
		return jsonIsUnknown
	}

	var res string
	switch json[0] {
	case '{':
		res = jsonIsBsonD
	case '[':
		res = jsonIsBsonA
	default:
		res = jsonIsUnknown
	}

	return res
}

func UnmarshalJson2Bson(b []byte, createIfEmpty bool) (any, error) {
	var res interface{}
	var err error

	switch jsonIsBsonDOrBsonA(string(b)) {
	case jsonIsBsonD:
		res, err = UnmarshalJson2BsonD(b, createIfEmpty)
	case jsonIsBsonA:
		res, err = UnmarshalJson2ArrayOfBsonD(b, createIfEmpty)
	default:
		err = errors.New("cannot determine if json is a map or array")
	}

	if err != nil {
		log.Error().Err(err).Str("json", string(b)).Msg("error unmarshalling bson")
	}
	return res, err
}

func UnmarshalJson2BsonD(b []byte, createIfEmpty bool) (bson.D, error) {
	const semLogContext = "mongo-json-util::unmarshal-json-2-bson-d"
	var err error

	o := New()

	if len(b) == 0 {
		if createIfEmpty {
			return bson.D{}, nil
		}
		return nil, nil
	}

	/*
		doc := bson.M{}
		err := json.Unmarshal(b, &doc)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			log.Error().Str("json", string(b)).Msg(semLogContext)
			return nil, err
		}
	*/

	err = json.Unmarshal(b, &o)
	// err := json.Unmarshal(b, &o)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		log.Error().Str("json", string(b)).Msg(semLogContext)
		return nil, err
	}

	d, err := o.ToBsonD()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		log.Error().Str("json", string(b)).Msg(semLogContext)
		return nil, err
	}

	if d == nil && createIfEmpty {
		return bson.D{}, nil
	}

	return d, nil
}

func UnmarshalJson2ArrayOfBsonD(b []byte, createIfEmpty bool) ([]bson.D, error) {
	const semLogContext = "mongo-json-util::unmarshal-json-2-bson-d-array"
	var err error

	if len(b) == 0 {
		if createIfEmpty {
			return []bson.D{}, nil
		}
		return nil, nil
	}

	var omaps []OrderedMap
	err = json.Unmarshal(b, &omaps)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		log.Error().Str("json", string(b)).Msg(semLogContext)
		return nil, err
	}

	var resp []bson.D
	for _, o := range omaps {
		d, err := o.ToBsonD()
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return nil, err
		}

		resp = append(resp, d)
	}

	if resp == nil && createIfEmpty {
		return []bson.D{}, nil
	}

	return resp, nil

}

type Pair struct {
	key   string
	value interface{}
}

func (kv *Pair) Key() string {
	return kv.key
}

func (kv *Pair) Value() interface{} {
	return kv.value
}

type ByPair struct {
	Pairs    []*Pair
	LessFunc func(a *Pair, j *Pair) bool
}

func (a ByPair) Len() int           { return len(a.Pairs) }
func (a ByPair) Swap(i, j int)      { a.Pairs[i], a.Pairs[j] = a.Pairs[j], a.Pairs[i] }
func (a ByPair) Less(i, j int) bool { return a.LessFunc(a.Pairs[i], a.Pairs[j]) }

type OrderedMap struct {
	keys       []string
	values     map[string]interface{}
	escapeHTML bool
}

func New() *OrderedMap {
	o := OrderedMap{}
	o.keys = []string{}
	o.values = map[string]interface{}{}
	o.escapeHTML = true
	return &o
}

func (o *OrderedMap) ToBsonD() (bson.D, error) {
	const semLogContext = "ordered-map::to-bson-d"
	var d bson.D
	if len(o.values) == 0 {
		return d, nil
	}

	var a bson.D
	for _, k := range o.Keys() {
		v := o.Values()[k]
		log.Trace().Str("type", fmt.Sprintf("%T", v)).Msg(semLogContext)
		switch tv := v.(type) {
		case OrderedMap:
			d1, err := tv.ToBsonD()
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return d, err
			}
			a = append(a, bson.E{Key: k, Value: d1})
		case []interface{}:
			val, err := adaptSlice(tv)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return nil, err
			}
			a = append(a, bson.E{Key: k, Value: val})
		case bson.A:
			val, err := adaptSlice(tv)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return nil, err
			}
			a = append(a, bson.E{Key: k, Value: val})
		default:
			a = append(a, bson.E{Key: k, Value: v})
		}
	}

	return a, nil
}

func adaptSlice(a []interface{}) ([]interface{}, error) {
	const semLogContext = "ordered-map::slice-to-bson-d"

	var newA []interface{}
	for i := range a {
		log.Trace().Str("type", fmt.Sprintf("%T", a[i])).Msg(semLogContext)
		switch ta := a[i].(type) {
		case OrderedMap:
			d1, err := ta.ToBsonD()
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return nil, err
			}
			newA = append(newA, d1)
		case []interface{}:
			val, err := adaptSlice(ta)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return nil, err
			}
			a = append(a, val)
		default:
			newA = append(newA, a[i])
		}
	}

	return newA, nil
}

func (o *OrderedMap) SetEscapeHTML(on bool) {
	o.escapeHTML = on
}

func (o *OrderedMap) Get(key string) (interface{}, bool) {
	val, exists := o.values[key]
	return val, exists
}

func (o *OrderedMap) Set(key string, value interface{}) {
	_, exists := o.values[key]
	if !exists {
		o.keys = append(o.keys, key)
	}
	o.values[key] = value
}

func (o *OrderedMap) Delete(key string) {
	// check key is in use
	_, ok := o.values[key]
	if !ok {
		return
	}
	// remove from keys
	for i, k := range o.keys {
		if k == key {
			o.keys = append(o.keys[:i], o.keys[i+1:]...)
			break
		}
	}
	// remove from values
	delete(o.values, key)
}

func (o *OrderedMap) Keys() []string {
	return o.keys
}

func (o *OrderedMap) Values() map[string]interface{} {
	return o.values
}

// SortKeys Sort the map keys using your sort func
func (o *OrderedMap) SortKeys(sortFunc func(keys []string)) {
	sortFunc(o.keys)
}

// Sort Sort the map using your sort func
func (o *OrderedMap) Sort(lessFunc func(a *Pair, b *Pair) bool) {
	pairs := make([]*Pair, len(o.keys))
	for i, key := range o.keys {
		pairs[i] = &Pair{key, o.values[key]}
	}

	sort.Sort(ByPair{pairs, lessFunc})

	for i, pair := range pairs {
		o.keys[i] = pair.key
	}
}

func (o *OrderedMap) UnmarshalJSON(b []byte) error {
	if o.values == nil {
		o.values = map[string]interface{}{}
	}
	// err := json.Unmarshal(b, &o.values)
	err := UnmarshalMongoJson(b, &o.values)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	if _, err = dec.Token(); err != nil { // skip '{'
		return err
	}
	o.keys = make([]string, 0, len(o.values))
	return decodeOrderedMap(dec, o)
}

func decodeOrderedMap(dec *json.Decoder, o *OrderedMap) error {
	hasKey := make(map[string]bool, len(o.values))
	for {
		token, err := dec.Token()
		if err != nil {
			return err
		}
		if delim, ok := token.(json.Delim); ok && delim == '}' {
			return nil
		}
		key := token.(string)
		if hasKey[key] {
			// duplicate key
			for j, k := range o.keys {
				if k == key {
					copy(o.keys[j:], o.keys[j+1:])
					break
				}
			}
			o.keys[len(o.keys)-1] = key
		} else {
			hasKey[key] = true
			o.keys = append(o.keys, key)
		}

		token, err = dec.Token()
		if err != nil {
			return err
		}
		if delim, ok := token.(json.Delim); ok {
			switch delim {
			case '{':
				if values, ok := o.values[key].(map[string]interface{}); ok {
					newMap := OrderedMap{
						keys:       make([]string, 0, len(values)),
						values:     values,
						escapeHTML: o.escapeHTML,
					}
					if err = decodeOrderedMap(dec, &newMap); err != nil {
						return err
					}
					o.values[key] = newMap
				} else if oldMap, ok := o.values[key].(OrderedMap); ok {
					newMap := OrderedMap{
						keys:       make([]string, 0, len(oldMap.values)),
						values:     oldMap.values,
						escapeHTML: o.escapeHTML,
					}
					if err = decodeOrderedMap(dec, &newMap); err != nil {
						return err
					}
					o.values[key] = newMap
				} else if err = decodeOrderedMap(dec, &OrderedMap{}); err != nil {
					return err
				}
			case '[':
				switch values := o.values[key].(type) {
				case []interface{}:
					if err = decodeSlice(dec, values, o.escapeHTML); err != nil {
						return err
					}
				case bson.A:
					if err = decodeSlice(dec, values, o.escapeHTML); err != nil {
						return err
					}
				default:
					if err = decodeSlice(dec, []interface{}{}, o.escapeHTML); err != nil {
						return err
					}
				}
				//if values, ok := o.values[key].([]interface{}); ok {
				//	if err = decodeSlice(dec, values, o.escapeHTML); err != nil {
				//		return err
				//	}
				//} else if err = decodeSlice(dec, []interface{}{}, o.escapeHTML); err != nil {
				//	return err
				//}
			}
		}
	}
}

func decodeSlice(dec *json.Decoder, s []interface{}, escapeHTML bool) error {
	for index := 0; ; index++ {
		token, err := dec.Token()
		if err != nil {
			return err
		}
		if delim, ok := token.(json.Delim); ok {
			switch delim {
			case '{':
				if index < len(s) {
					if values, ok := s[index].(map[string]interface{}); ok {
						newMap := OrderedMap{
							keys:       make([]string, 0, len(values)),
							values:     values,
							escapeHTML: escapeHTML,
						}
						if err = decodeOrderedMap(dec, &newMap); err != nil {
							return err
						}
						s[index] = newMap
					} else if oldMap, ok := s[index].(OrderedMap); ok {
						newMap := OrderedMap{
							keys:       make([]string, 0, len(oldMap.values)),
							values:     oldMap.values,
							escapeHTML: escapeHTML,
						}
						if err = decodeOrderedMap(dec, &newMap); err != nil {
							return err
						}
						s[index] = newMap
					} else if err = decodeOrderedMap(dec, &OrderedMap{}); err != nil {
						return err
					}
				} else if err = decodeOrderedMap(dec, &OrderedMap{}); err != nil {
					return err
				}
			case '[':
				if index < len(s) {
					if values, ok := s[index].([]interface{}); ok {
						if err = decodeSlice(dec, values, escapeHTML); err != nil {
							return err
						}
					} else if err = decodeSlice(dec, []interface{}{}, escapeHTML); err != nil {
						return err
					}
				} else if err = decodeSlice(dec, []interface{}{}, escapeHTML); err != nil {
					return err
				}
			case ']':
				return nil
			}
		}
	}
}

func (o OrderedMap) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(o.escapeHTML)
	for i, k := range o.keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		// add key
		if err := encoder.Encode(k); err != nil {
			return nil, err
		}
		buf.WriteByte(':')
		// add value
		if err := encoder.Encode(o.values[k]); err != nil {
			return nil, err
		}
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func UnmarshalMongoJson(data []byte, v any) error {
	vr, err := bson.NewExtJSONValueReader(bytes.NewReader(data), false)
	if err != nil {
		return err
	}

	decoder := bson.NewDecoder(vr)
	// V2 doesn't return an error.
	//decoder, err := bson.NewDecoder(vr)
	//if err != nil {
	//	return err
	//}

	err = decoder.Decode(v)
	if err != nil {
		return err
	}

	return nil
}

func JsonExtended2JsonConv(data []byte) ([]byte, error) {
	const semLogContext = "mongo-json-util::json-ext-2-json"
	var m bson.M
	err := UnmarshalMongoJson(data, &m)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	b, err := json.Marshal(m)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	return b, nil
}
