package keystring

const (
	ModeTopLevel = "toplevel"
	ModeSingle   = "single"
	ModeNamed    = "named"

	KeyStringVersionV0 = "v0"
	KeyStringVersionV1 = "v1"

	kEnd     = 4
	kLess    = 1
	kGreater = 254
)

const (
	kMinKey        = 10
	kUndefined     = 15
	kNullish       = 20
	kNumeric       = 30
	kStringLike    = 60
	kObject        = 70
	kArray         = 80
	kBinData       = 90
	kOID           = 100
	kBool          = 110
	kDate          = 120
	kTimestamp     = 130
	kRegEx         = 140
	kDBRef         = 150
	kCode          = 160
	kCodeWithScope = 170
	kMaxKey        = 240

	kNumericNaN                    = kNumeric + 0
	kNumericNegativeLargeMagnitude = kNumeric + 1 // <= -2**63 including -Inf
	kNumericNegative8ByteInt       = kNumeric + 2
	kNumericNegative7ByteInt       = kNumeric + 3
	kNumericNegative6ByteInt       = kNumeric + 4
	kNumericNegative5ByteInt       = kNumeric + 5
	kNumericNegative4ByteInt       = kNumeric + 6
	kNumericNegative3ByteInt       = kNumeric + 7
	kNumericNegative2ByteInt       = kNumeric + 8
	kNumericNegative1ByteInt       = kNumeric + 9
	kNumericNegativeSmallMagnitude = kNumeric + 10 // between 0 and -1 exclusive
	kNumericZero                   = kNumeric + 11
	kNumericPositiveSmallMagnitude = kNumeric + 12 // between 0 and 1 exclusive
	kNumericPositive1ByteInt       = kNumeric + 13
	kNumericPositive2ByteInt       = kNumeric + 14
	kNumericPositive3ByteInt       = kNumeric + 15
	kNumericPositive4ByteInt       = kNumeric + 16
	kNumericPositive5ByteInt       = kNumeric + 17
	kNumericPositive6ByteInt       = kNumeric + 18
	kNumericPositive7ByteInt       = kNumeric + 19
	kNumericPositive8ByteInt       = kNumeric + 20
	kNumericPositiveLargeMagnitude = kNumeric + 21 // >= 2**63 including +Inf

	kBoolFalse = kBool + 0
	kBoolTrue  = kBool + 1
)

type KeyString struct {
	buf     *Buffer
	version string
}

func NewKeyStringFromString(version string, s string) (KeyString, error) {
	buf, err := NewBufferFromString(s)
	if err != nil {
		return KeyString{}, err
	}
	return KeyString{buf: &buf, version: version}, nil
}

func (ks *KeyString) readCType() (byte, error) {
	ctype, err := ks.buf.readByte()
	if err != nil {
		return ctype, err
	}

	if ctype == kLess || ctype == kGreater {
		ctype, err = ks.buf.readByte()
	}

	return ctype, err
}

func numBytesForInt(ctype int) int {
	if ctype >= kNumericPositive1ByteInt {
		return ctype - kNumericPositive1ByteInt + 1
	}

	return kNumericNegative1ByteInt - ctype + 1
}

func (ks *KeyString) readValue(ctype byte) (interface{}, error) {

	isNegative := false
	switch ctype {
	case kUndefined:
		return nil, nil
	case kBoolTrue:
		return true, nil
	case kBoolFalse:
		return false, nil
	case kTimestamp:
		return ks.readTimestamp()
	case kOID:
		return ks.readObjectId()
	case kStringLike:
		return ks.buf.readCStringWithNulls()
	case kBinData:
		return ks.readBinData()
	case kObject:
		return ks.ToMapOfValues()
	case kNumericNaN:
	case kNumericZero:
		return 0, nil
	case kNumericNegativeLargeMagnitude:
		isNegative = true
		fallthrough
	case kNumericPositiveLargeMagnitude:
	case kNumericNegativeSmallMagnitude:
		isNegative = true
		fallthrough
	case kNumericPositiveSmallMagnitude:
	case kNumericNegative8ByteInt:
		fallthrough
	case kNumericNegative7ByteInt:
		fallthrough
	case kNumericNegative6ByteInt:
		fallthrough
	case kNumericNegative5ByteInt:
		fallthrough
	case kNumericNegative4ByteInt:
		fallthrough
	case kNumericNegative3ByteInt:
		fallthrough
	case kNumericNegative2ByteInt:
		fallthrough
	case kNumericNegative1ByteInt:
		isNegative = true
		fallthrough

	case kNumericPositive1ByteInt:
		fallthrough
	case kNumericPositive2ByteInt:
		fallthrough
	case kNumericPositive3ByteInt:
		fallthrough
	case kNumericPositive4ByteInt:
		fallthrough
	case kNumericPositive5ByteInt:
		fallthrough
	case kNumericPositive6ByteInt:
		fallthrough
	case kNumericPositive7ByteInt:
		fallthrough
	case kNumericPositive8ByteInt:
		var encodedIntegerPart int64
		intBytesRemaining := numBytesForInt(int(ctype))
		for intBytesRemaining > 0 {
			b, _ := ks.buf.readByte()
			if isNegative {
				b = ^b & 0xff
			}
			encodedIntegerPart = (encodedIntegerPart << 8) + int64(b)
			intBytesRemaining--
		}
		haveFractionalPart := (encodedIntegerPart & 1) != 0
		integerPart := encodedIntegerPart >> 1
		if !haveFractionalPart {
			if isNegative {
				integerPart = -integerPart
			}
			/*
				if Number.isSafeInteger(Number(integerPart)) {
					return Number(integerPart)
				}
			*/
			return integerPart, nil
		}
	}

	return nil, nil
}

func (ks *KeyString) ToSingleValueBsonPartial() (any, error) {

	var value interface{}

	for ks.buf.bytesAvailable(1) {
		ctype, err := ks.readCType()
		if err != nil {
			return nil, err
		}

		if ctype == kEnd {
			break
		}

		value, err = ks.readValue(ctype)
		return value, err
	}

	return nil, nil
}

func (ks *KeyString) ToMapOfValues() (any, error) {

	var mapOfElems map[string]interface{}
	for ks.buf.bytesAvailable(1) {
		ctype, err := ks.readCType()
		if err != nil {
			return nil, err
		}

		if ctype == kEnd {
			break
		}

		if ctype == 0 {
			break
		}
		key, err := ks.buf.readCString()
		if err != nil {
			return nil, err
		}
		ctype, err = ks.readCType()
		if err != nil {
			return nil, err
		}
		singleValue, err := ks.readValue(ctype)
		if err != nil {
			return nil, err
		}
		if mapOfElems == nil {
			mapOfElems = make(map[string]interface{})
		}
		mapOfElems[key] = singleValue
	}

	return mapOfElems, nil
}

func (ks *KeyString) ArrayOfValuesKeyStringToBsonPartial(version string, buf *Buffer) (any, error) {

	var arrayOfElems []interface{}
	for buf.bytesAvailable(1) {
		ctype, err := ks.readCType()
		if err != nil {
			return nil, err
		}

		if ctype == kEnd {
			break
		}

		singleValue, err := ks.readValue(ctype)
		if err != nil {
			return nil, err
		}

		arrayOfElems = append(arrayOfElems, singleValue)
	}

	return arrayOfElems, nil
}
