package lease

import (
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"go.mongodb.org/mongo-driver/bson"
	"time"
)

// @tpm-schematics:start-region("top-file-section")

import (
	"github.com/rs/zerolog/log"
	"strings"
)

const (
	EntityType = "lease"

	StatusLeased    = "leased"
	StatusAvailable = "available"
	StatusError     = "error"
)

// @tpm-schematics:end-region("top-file-section")

type Lease struct {
	Bid          string `json:"_bid,omitempty" bson:"_bid,omitempty" yaml:"_bid,omitempty"`
	Et           string `json:"_et,omitempty" bson:"_et,omitempty" yaml:"_et,omitempty"`
	Gid          string `json:"_gid,omitempty" bson:"_gid,omitempty" yaml:"_gid,omitempty"`
	LeaseId      string `json:"leaseId,omitempty" bson:"leaseId,omitempty" yaml:"leaseId,omitempty"`
	Data         bson.M `json:"data,omitempty" bson:"data,omitempty" yaml:"data,omitempty"`
	Status       string `json:"status,omitempty" bson:"status,omitempty" yaml:"status,omitempty"`
	Etag         int64  `json:"etag" bson:"etag" yaml:"etag"`
	Duration     int32  `json:"duration_sec,omitempty" bson:"duration_sec,omitempty" yaml:"duration_sec,omitempty"`
	Ts           string `json:"ts,omitempty" bson:"ts,omitempty" yaml:"ts,omitempty"`
	Ttl          int32  `json:"ttl,omitempty" bson:"ttl,omitempty" yaml:"ttl,omitempty"`
	Errors       int32  `json:"errors,omitempty" bson:"errors,omitempty" yaml:"errors,omitempty"`
	Acquisitions int32  `json:"acquisitions,omitempty" bson:"acquisitions,omitempty" yaml:"acquisitions,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s Lease) IsZero() bool {
	return s.Bid == "" && s.Et == "" && s.Gid == "" && s.LeaseId == "" && len(s.Data) == 0 && s.Status == "" && s.Etag == 0 && s.Duration == 0 && s.Ts == "" && s.Ttl == 0 && s.Errors == 0 && s.Acquisitions == 0
}

// @tpm-schematics:start-region("bottom-file-section")

func NewLease(leaseType string, objId string, status string, durationSecs int32) Lease {

	leasedObjectId := objId // LeasedObjectId(leaseType, objId)
	lid := strings.Join([]string{leaseType, objId, util.NewObjectId().String()}, ":")

	l := Lease{
		Bid:          leasedObjectId,
		Et:           EntityType,
		Gid:          leaseType,
		LeaseId:      lid,
		Status:       status,
		Duration:     durationSecs,
		Ts:           time.Now().Format(time.RFC3339Nano),
		Ttl:          300,
		Acquisitions: 1,
	}

	return l
}

var leasedObjectNamePattern = "evt-lease:%s:%s"

func LeasedObjectId(leaseType string, objId string) string {
	return fmt.Sprintf(leasedObjectNamePattern, leaseType, objId)
}

func (l Lease) Acquirable() bool {
	return l.Status == StatusAvailable || (l.Status == StatusLeased && l.Expired())
	// return !(l.Status == "leased" && !l.Expired())
}

func (l Lease) Expired() bool {
	const semLogContext = "lease::expired"

	if l.Ts != "" && l.Duration > 0 {
		ts, err := time.Parse(time.RFC3339Nano, l.Ts)
		if err != nil {
			log.Error().Err(err).Str("ts", l.Ts).Msg(semLogContext)
			return true
		}

		var td time.Duration
		td = time.Duration(l.Duration) * time.Second
		if time.Now().Sub(ts) > td {
			return true
		}
		return false
	}

	return true
}

func (l Lease) Acquired() Lease {
	const semLogContext = "lease::acquired"

	l.LeaseId = strings.Join([]string{l.Gid, l.Bid, util.NewObjectId().String()}, ":")
	l.Status = StatusLeased
	l.Ts = time.Now().Format(time.RFC3339Nano)
	l.Etag++

	return l
}

func (l Lease) Renewed() Lease {
	const semLogContext = "lease::renewed"

	l.Status = StatusLeased
	l.Ts = time.Now().Format(time.RFC3339Nano)
	l.Etag++

	return l
}

func (l Lease) Cleared() Lease {
	const semLogContext = "lease::cleared"

	l.Status = StatusAvailable

	l.Ts = time.Now().Format(time.RFC3339Nano)
	l.Etag++

	return l
}

// @tpm-schematics:end-region("bottom-file-section")
