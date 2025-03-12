package lease

import (
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type Handler struct {
	cli         *mongo.Collection
	Lease       Lease
	auto        bool
	autoRenewCh chan struct{}
}

func (lh *Handler) IsZero() bool {
	return lh.Lease.Bid == ""
}

// findLeaseByLeasedObjectId (ctx context.Context, client *mongo.Collection, lid string) (Lease, error) {
func findLeaseByGroupIdAndLeasedObjectId(coll *mongo.Collection, leaseGroupId string, leasedObjectId string) (Lease, error) {
	const semLogContext = "lease::find-by-type-and-bid"
	var doc Lease

	f := Filter{}
	f.Or().AndBidEqTo(leasedObjectId).AndGidEqTo(leaseGroupId).AndEtEqTo(EntityType)
	opts := options.FindOneOptions{}
	err := coll.FindOne(context.Background(), f.Build(), &opts).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			log.Info().Str("lease-gid", leaseGroupId).Str("leased-object-id", leasedObjectId).Msg(semLogContext + " - no lease found")
			return Lease{}, nil
		} else {
			log.Error().Err(err).Str("lease-gid", leaseGroupId).Str("leased-object-id", leasedObjectId).Msg(semLogContext)
		}
	}

	return doc, nil
}

func CanAcquireLease(coll *mongo.Collection, leaseGroupId, leasedObjectId string) (bool, error) {
	const semLogContext = "lease::can-acquire-lease"
	d, err := findLeaseByGroupIdAndLeasedObjectId(coll, leaseGroupId, leasedObjectId)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return false, err
	}

	if d.IsZero() {
		return true, nil
	}

	return d.Acquirable(), nil
}

func AcquireLease(client *mongo.Collection, leaseGroupId, leasedObjectId string, auto bool) (*Handler, bool, error) {

	const semLogContext = "lease::acquire-lease"

	l, err := findLeaseByGroupIdAndLeasedObjectId(client, leaseGroupId, leasedObjectId)
	if err != nil {
		return nil, false, err
	}

	if l.IsZero() {
		l = NewLease(leaseGroupId, leasedObjectId, "leased", 60)
		_, err = client.InsertOne(context.Background(), l)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return nil, false, err
		}
	} else {
		if !l.Acquirable() {
			log.Info().Str("status", l.Status).Str("lease-type", leaseGroupId).Str("leased-object-id", leasedObjectId).Msg(semLogContext + " - lease cannot be acquired")
			return nil, false, nil
		} else {
			f := Filter{}
			f.Or().AndBidEqTo(leasedObjectId).AndGidEqTo(leaseGroupId).AndEtagEqTo(l.Etag).AndEtEqTo(EntityType)

			l = l.Acquired()
			ud := GetUpdateDocumentFromOptions(
				UpdateWithLeaseId(l.LeaseId),
				UpdateWithStatus(l.Status),
				UpdateWithTs(l.Ts),
				UpdateWithEtag(l.Etag),
			)
			res, err := client.UpdateOne(context.Background(), f.Build(), ud.Build())
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return nil, false, err
			}

			if res.ModifiedCount == 0 {
				log.Info().Str("lease-type", leaseGroupId).Str("leased-object-id", leasedObjectId).Msg(semLogContext + " - failed lease acquiring")
				return nil, false, nil
			}
		}
	}

	lh := Handler{
		cli:         client,
		Lease:       l,
		auto:        auto,
		autoRenewCh: make(chan struct{}),
	}

	if auto {
		go lh.renewLoop()
	}

	return &lh, true, nil
}

func (lh *Handler) SetLeaseData(n string, s interface{}) {
	//var err error
	if lh.Lease.Data == nil {
		lh.Lease.Data = make(bson.M)
	}

	lh.Lease.Data[n] = s
	//if forceRenew {
	//	err = lh.RenewLease()
	//}

	//return err
}

func (lh *Handler) GetLeaseData(n string, defaultValue interface{}) interface{} {
	if lh.Lease.Data == nil {
		return defaultValue
	}

	v, ok := lh.Lease.Data[n]
	if !ok {
		return defaultValue
	}

	return v
}

func (lh *Handler) GetLeaseStringData(n string, defaultValue string) string {
	if lh.Lease.Data == nil {
		return defaultValue
	}

	v, ok := lh.Lease.Data[n]
	if !ok {
		return defaultValue
	}

	return fmt.Sprint(v)
}

func (lh *Handler) Release() error {

	const semLogContext = "lease-handler::release"

	d, err := findLeaseByGroupIdAndLeasedObjectId(lh.cli, lh.Lease.Gid, lh.Lease.Bid)
	if err != nil {
		log.Error().Err(err).Interface("lease", lh.Lease).Msg(semLogContext)
		return err
	}

	if d.LeaseId != lh.Lease.LeaseId {
		log.Warn().Interface("lease", lh.Lease).Msg(semLogContext + " lease id already been released")
		return nil
	}

	f := Filter{}
	f.Or().AndBidEqTo(lh.Lease.Bid).AndGidEqTo(lh.Lease.Gid).AndEtagEqTo(lh.Lease.Etag)

	lh.Lease = lh.Lease.Cleared()
	ud := GetUpdateDocumentFromOptions(
		UpdateWithStatus(lh.Lease.Status),
		UpdateWithEtag(lh.Lease.Etag),
		UpdateWithTs(lh.Lease.Ts),
	)

	res, err := lh.cli.UpdateOne(context.Background(), f.Build(), ud.Build())
	if lh.auto {
		close(lh.autoRenewCh)
	}

	if err != nil {
		log.Error().Interface("lease", lh.Lease).Err(err).Msg(semLogContext)
		return err
	}

	if res.ModifiedCount == 0 {
		log.Error().Interface("lease", lh.Lease).Msg(semLogContext + " lease already released")
	} else {
		log.Info().Interface("lease", lh.Lease).Msg(semLogContext + " lease released")
	}

	return err
}

func (lh *Handler) renewLoop() {
	const semLogContext = "lease-handler::renew-loop"

	tickInterval := time.Second * time.Duration(float64(lh.Lease.Duration)*0.6)
	log.Info().Float64("tickInterval-secs", tickInterval.Seconds()).Msg(semLogContext + " starting...")

	ticker := time.NewTicker(tickInterval)
	var exitLoop bool
	for !exitLoop {
		select {
		case <-ticker.C:
			err := lh.RenewLease()
			if err != nil {
				log.Error().Err(err)
			}
		case <-lh.autoRenewCh:
			ticker.Stop()
			exitLoop = true
		}
	}

	log.Info().Msg(semLogContext + " ended")
}

func (lh *Handler) RenewLease() error {
	const semLogContext = "lease-handler::renew"

	d, err := findLeaseByGroupIdAndLeasedObjectId(lh.cli, lh.Lease.Gid, lh.Lease.Bid)
	if err != nil {
		return err
	}

	if d.LeaseId != lh.Lease.LeaseId {
		err := fmt.Errorf("lease-id on object %s: wanted %s, actual %s", lh.Lease.Bid, lh.Lease.LeaseId, d.LeaseId)
		return err
	}

	f := Filter{}
	f.Or().AndBidEqTo(lh.Lease.Bid).AndGidEqTo(lh.Lease.Gid).AndEtagEqTo(lh.Lease.Etag).AndEtEqTo(EntityType)
	lh.Lease = lh.Lease.Renewed()

	updOptions := []UpdateOption{
		UpdateWithStatus(lh.Lease.Status),
		UpdateWithEtag(lh.Lease.Etag),
		UpdateWithTs(lh.Lease.Ts),
		UpdateWithData(lh.Lease.Data),
	}
	ud := GetUpdateDocumentFromOptions(updOptions...)

	res, err := lh.cli.UpdateOne(context.Background(), f.Build(), ud.Build())
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	if res.ModifiedCount == 0 {
		log.Error().Msg(semLogContext + " lease already released")
	}

	return err
}
