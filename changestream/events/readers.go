package events

import (
	"fmt"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func getString(m bson.M, fieldName string, mandatory bool) (string, error) {

	var err error
	i, ok := m[fieldName]
	if !ok {
		if mandatory {
			err = fmt.Errorf("missing %s", fieldName)
		}
		return "", err
	}

	v, ok := i.(string)
	if !ok {
		err = fmt.Errorf("invalid type %T fot %s", i, fieldName)
		return "", err
	}

	return v, nil
}

func getNumberLong(m bson.M, fieldName string, mandatory bool) (int64, error) {

	var err error
	i, ok := m[fieldName]
	if !ok {
		if mandatory {
			err = fmt.Errorf("missing %s", fieldName)
		}
		return 0, err
	}

	v, ok := i.(int64)
	if !ok {
		err = fmt.Errorf("invalid type %T fot %s", i, fieldName)
		return 0, err
	}

	return v, nil
}

func getTimestamp(m bson.M, fieldName string, mandatory bool) (bson.Timestamp, error) {

	var err error
	i, ok := m[fieldName]
	if !ok {
		if mandatory {
			err = fmt.Errorf("missing %s", fieldName)
		}
		return bson.Timestamp{}, err
	}

	v, ok := i.(bson.Timestamp)
	if !ok {
		err = fmt.Errorf("invalid type %T fot %s", i, fieldName)
		return bson.Timestamp{}, err
	}

	return v, nil
}

func getDateTime(m bson.M, fieldName string, mandatory bool) (bson.DateTime, error) {

	var err error
	i, ok := m[fieldName]
	if !ok {
		if mandatory {
			err = fmt.Errorf("missing %s", fieldName)
		}
		return 0, err
	}

	v, ok := i.(bson.DateTime)
	if !ok {
		err = fmt.Errorf("invalid type %T fot %s", i, fieldName)
		return 0, err
	}

	return v, nil
}

func getDocument(m bson.M, fieldName string, mandatory bool) (bson.M, error) {

	var err error
	i, ok := m[fieldName]
	if !ok {
		if mandatory {
			err = fmt.Errorf("missing %s", fieldName)
		}
		return nil, err
	}

	if i == nil {
		if mandatory {
			err = fmt.Errorf("missing %s", fieldName)
			return nil, err
		}

		return nil, err
	}

	v, ok := i.(bson.M)
	if !ok {
		err = fmt.Errorf("invalid type %T for %s", i, fieldName)
		return nil, err
	}

	return v, nil
}

type Namespace struct {
	Db   string `yaml:"db,omitempty" mapstructure:"db,omitempty" json:"db,omitempty"`
	Coll string `yaml:"coll,omitempty" mapstructure:"coll,omitempty" json:"coll,omitempty"`
}

func getNamespace(m bson.M, mandatory bool) (Namespace, error) {

	var err error
	d, err := getDocument(m, "ns", mandatory)
	if err != nil {
		return Namespace{}, err
	}

	ns := Namespace{}
	ns.Db, err = getString(d, "db", true)
	if err == nil {
		ns.Coll, err = getString(d, "coll", true)
	}

	return ns, nil
}
