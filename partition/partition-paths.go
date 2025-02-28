package partition

// This file contains the paths to the field in the generated entity.
// A path is a string with all the identifiers from the root document to the single leaves.
// In case of maps and arrays place holder for the key (%s) or the index %d have been provided.

// @tpm-schematics:start-region("top-file-section")
// @tpm-schematics:end-region("top-file-section")

const (
	BidFieldName              = "_bid"
	GidFieldName              = "_gid"
	EtFieldName               = "_et"
	PartitionNumberFieldName  = "partitionNumber"
	StatusFieldName           = "status"
	EtagFieldName             = "etag"
	MongoFieldName            = "mongo"
	Mongo_InstanceFieldName   = "mongo.instance"
	Mongo_CollectionFieldName = "mongo.collection"
	Mongo_FilterFieldName     = "mongo.filter"
)

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
