package events

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func getField(m any, fieldName string) (any, bool, error) {
	var field any
	var err error
	var ok bool

	switch container := m.(type) {
	case bson.M:
		field, ok = container[fieldName]
	case bson.D:
		for _, f := range container {
			if f.Key == fieldName {
				ok = true
				field = f.Value
				break
			}
		}

	default:
		err = fmt.Errorf("unknown container type %T for %s", container, fieldName)
	}

	return field, ok, err
}

func getString(m any, fieldName string, mandatory bool) (string, error) {

	field, ok, err := getField(m, fieldName)
	if err != nil {
		return "", err
	}

	if !ok {
		if mandatory {
			err = fmt.Errorf("missing %s", fieldName)
			return "", err
		}
		return "", nil
	}

	v, ok := field.(string)
	if !ok {
		err = fmt.Errorf("invalid type %T fot %s", field, fieldName)
		return "", err
	}

	return v, nil
}

func getNumberLong(m any, fieldName string, mandatory bool) (int64, error) {

	field, ok, err := getField(m, fieldName)
	if err != nil {
		return 0, err
	}

	if !ok {
		if mandatory {
			err = fmt.Errorf("missing %s", fieldName)
			return 0, err
		}
		return 0, nil
	}

	v, ok := field.(int64)
	if !ok {
		err = fmt.Errorf("invalid type %T fot %s", field, fieldName)
		return 0, err
	}

	return v, nil
}

func getTimestamp(m bson.M, fieldName string, mandatory bool) (bson.Timestamp, error) {

	field, ok, err := getField(m, fieldName)
	if err != nil {
		return bson.Timestamp{}, err
	}

	if !ok {
		if mandatory {
			err = fmt.Errorf("missing %s", fieldName)
			return bson.Timestamp{}, err
		}
		return bson.Timestamp{}, nil
	}

	v, ok := field.(bson.Timestamp)
	if !ok {
		err = fmt.Errorf("invalid type %T fot %s", field, fieldName)
		return bson.Timestamp{}, err
	}

	return v, nil
}

func getDateTime(m bson.M, fieldName string, mandatory bool) (bson.DateTime, error) {

	field, ok, err := getField(m, fieldName)
	if err != nil {
		return 0, err
	}

	if !ok {
		if mandatory {
			err = fmt.Errorf("missing %s", fieldName)
			return 0, err
		}
		return 0, nil
	}

	v, ok := field.(bson.DateTime)
	if !ok {
		err = fmt.Errorf("invalid type %T fot %s", field, fieldName)
		return 0, err
	}

	return v, nil
}

type Namespace struct {
	Db   string `yaml:"db,omitempty" mapstructure:"db,omitempty" json:"db,omitempty"`
	Coll string `yaml:"coll,omitempty" mapstructure:"coll,omitempty" json:"coll,omitempty"`
}

func getDocument(m any, fieldName string, mandatory bool) (bson.D, error) {

	field, ok, err := getField(m, fieldName)
	if err != nil {
		return bson.D{}, err
	}

	if !ok {
		if mandatory {
			err = fmt.Errorf("missing %s", fieldName)
			return bson.D{}, err
		}
		return bson.D{}, nil
	}

	v, ok := field.(bson.D)
	if !ok {
		err = fmt.Errorf("invalid type %T for %s", field, fieldName)
		return nil, err
	}

	return v, nil
}

func getMap(m any, fieldName string, mandatory bool) (bson.M, error) {

	field, ok, err := getField(m, fieldName)
	if err != nil {
		return nil, err
	}

	if !ok {
		if mandatory {
			err = fmt.Errorf("missing %s", fieldName)
			return nil, err
		}
		return nil, nil
	}

	v, ok := field.(bson.M)
	if !ok {
		err = fmt.Errorf("invalid type %T for %s", field, fieldName)
		return nil, err
	}

	return v, nil
}

func getNamespace(m any, mandatory bool) (Namespace, error) {

	nsDoc, ok, err := getField(m, "ns")
	if err != nil {
		return Namespace{}, err
	}

	if !ok {
		if mandatory {
			err = fmt.Errorf("missing %s", "ns")
			return Namespace{}, err
		}
		return Namespace{}, nil
	}

	ns := Namespace{}
	ns.Db, err = getString(nsDoc, "db", true)
	if err == nil {
		ns.Coll, err = getString(nsDoc, "coll", true)
	}

	return ns, nil
}
