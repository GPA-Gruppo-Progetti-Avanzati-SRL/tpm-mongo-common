package keystring

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (ks *KeyString) readTimestamp() (primitive.Timestamp, error) {
	t, err := ks.buf.readUint32BE()
	if err != nil {
		return primitive.Timestamp{}, err
	}
	i, err := ks.buf.readUint32BE()
	if err != nil {
		return primitive.Timestamp{}, err
	}
	return primitive.Timestamp{T: t, I: i}, nil
}

func (ks *KeyString) readObjectId() (interface{}, error) {
	b, err := ks.buf.readBytes(12)
	if err != nil {
		return primitive.NilObjectID, err
	}

	return primitive.ObjectID(b), nil
}

func (ks *KeyString) readBinData() (interface{}, error) {
	b, err := ks.buf.readByte()
	if err != nil {
		return primitive.Binary{}, err
	}

	size := uint32(b)
	if b == 0xff {
		size, err = ks.buf.readUint32BE()
		if err != nil {
			return primitive.Binary{}, err
		}
	}

	subtype, err := ks.buf.readByte()
	if err != nil {
		return primitive.Binary{}, err
	}

	binData, err := ks.buf.readBytes(int(size))
	if err != nil {
		return primitive.Binary{}, err
	}

	return primitive.Binary{
		Subtype: subtype,
		Data:    binData,
	}, nil
}
