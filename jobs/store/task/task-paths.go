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
	AmbitFieldName              = "ambit"
	DataSourceTypeFieldName     = "data_source_type"
	StreamTypeFieldName         = "stream_type"
	ProcessorIdFieldName        = "processor_id"
	JobIdFieldName              = "job_id"
	InfoFieldName               = "info"
	Info_MdbInstanceFieldName   = "info.mdbInstance"
	Info_MdbCollectionFieldName = "info.mdbCollection"
	Info_MdbBatchSizeFieldName  = "info.mdbBatchSize"
	PartitionsFieldName         = "partitions"
	Partitions_IFieldName       = "partitions.%d"
)

// @tpm-schematics:start-region("bottom-file-section")

const (
	PartitionsIStatusFieldName       = "partitions.%d.status"
	PartitionsIErrorsFieldName       = "partitions.%d.errors"
	PartitionsIAcquisitionsFieldName = "partitions.%d.acquisitions"
)

// @tpm-schematics:end-region("bottom-file-section")
