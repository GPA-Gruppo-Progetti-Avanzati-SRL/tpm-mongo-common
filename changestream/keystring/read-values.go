package keystring

import (
	"go.mongodb.org/mongo-driver/v2/bson"
)

func (ks *KeyString) readTimestamp() (bson.Timestamp, error) {
	t, err := ks.buf.readUint32BE()
	if err != nil {
		return bson.Timestamp{}, err
	}
	i, err := ks.buf.readUint32BE()
	if err != nil {
		return bson.Timestamp{}, err
	}
	return bson.Timestamp{T: t, I: i}, nil
}

func (ks *KeyString) readObjectId() (interface{}, error) {
	b, err := ks.buf.readBytes(12)
	if err != nil {
		return bson.NilObjectID, err
	}

	return bson.ObjectID(b), nil
}

func (ks *KeyString) readBinData() (interface{}, error) {
	b, err := ks.buf.readByte()
	if err != nil {
		return bson.Binary{}, err
	}

	size := uint32(b)
	if b == 0xff {
		size, err = ks.buf.readUint32BE()
		if err != nil {
			return bson.Binary{}, err
		}
	}

	subtype, err := ks.buf.readByte()
	if err != nil {
		return bson.Binary{}, err
	}

	binData, err := ks.buf.readBytes(int(size))
	if err != nil {
		return bson.Binary{}, err
	}

	return bson.Binary{
		Subtype: subtype,
		Data:    binData,
	}, nil
}
