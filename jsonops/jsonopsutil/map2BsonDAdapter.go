package jsonopsutil

import (
	"bytes"
	"encoding/json"
	"go.mongodb.org/mongo-driver/bson"
)

type Map2BsonDAdapter struct {
	Values map[string]interface{}
	Keys   []string
}

func NewMap2BsonDAdapter() Map2BsonDAdapter {
	o := Map2BsonDAdapter{
		Keys:   nil,
		Values: nil,
	}
	return o
}

func (o *Map2BsonDAdapter) GetDocument() (bson.D, error) {
	if len(o.Values) > 0 {
		var a bson.D
		for _, k := range o.Keys {
			v := o.Values[k]
			a = append(a, bson.E{Key: k, Value: v})
		}
		return a, nil
	}

	return nil, nil
}

func (o *Map2BsonDAdapter) UnmarshalJSON(b []byte) error {
	if o.Values == nil {
		o.Values = map[string]interface{}{}
	}
	err := json.Unmarshal(b, &o.Values)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	if _, err = dec.Token(); err != nil { // skip '{'
		return err
	}
	o.Keys = make([]string, 0, len(o.Values))
	return decodeMap(dec, o)
}

func decodeMap(dec *json.Decoder, o *Map2BsonDAdapter) error {
	hasKey := make(map[string]bool, len(o.Values))
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
		} else {
			hasKey[key] = true
			o.Keys = append(o.Keys, key)
		}

		token, err = dec.Token()
		if err != nil {
			return err
		}
		if delim, ok := token.(json.Delim); ok {
			switch delim {
			case '{':
				return skipMap(dec)
			case '[':
				return skipSlice(dec)
			}
		}
	}
}

func skipMap(dec *json.Decoder) error {
	for {
		token, err := dec.Token()
		if err != nil {
			return err
		}
		if delim, ok := token.(json.Delim); ok && delim == '}' {
			return nil
		}

		token, err = dec.Token()
		if err != nil {
			return err
		}
		if delim, ok := token.(json.Delim); ok {
			switch delim {
			case '{':
				return skipMap(dec)
			case '[':
				return skipSlice(dec)
			}
		}
	}
}

func skipSlice(dec *json.Decoder) error {
	for index := 0; ; index++ {
		token, err := dec.Token()
		if err != nil {
			return err
		}
		if delim, ok := token.(json.Delim); ok {
			switch delim {
			case '{':
				return skipMap(dec)

			case '[':
				return skipSlice(dec)
			case ']':
				return nil
			}
		}
	}

}
