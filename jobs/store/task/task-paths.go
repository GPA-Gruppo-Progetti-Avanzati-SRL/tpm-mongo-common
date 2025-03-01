package task

// This file contains the paths to the field in the generated entity.
// A path is a string with all the identifiers from the root document to the single leaves.
// In case of maps and arrays place holder for the key (%s) or the index %d have been provided.

// @tpm-schematics:start-region("top-file-section")
// @tpm-schematics:end-region("top-file-section")

const (
	BidFieldName                = "_bid"
	EtFieldName                 = "_et"
	StatusFieldName             = "status"
	TypFieldName                = "typ"
	JobBidFieldName             = "jobBid"
	InfoFieldName               = "info"
	Info_MdbInstanceFieldName   = "info.mdbInstance"
	Info_MdbCollectionFieldName = "info.mdbCollection"
	PartitionsFieldName         = "partitions"
	Partitions_IFieldName       = "partitions.%d"
)

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
