package tasklog

// This file contains the paths to the field in the generated entity.
// A path is a string with all the identifiers from the root document to the single leaves.
// In case of maps and arrays place holder for the key (%s) or the index %d have been provided.

// @tpm-schematics:start-region("top-file-section")
// @tpm-schematics:end-region("top-file-section")

const (
	DomainFieldName      = "domain"
	SiteFieldName        = "site"
	BidFieldName         = "_bid"
	EtFieldName          = "_et"
	NameFieldName        = "name"
	TaskIdFieldName      = "task_id"
	PartitionFieldName   = "partition"
	JobIdFieldName       = "job_id"
	BlockNumberFieldName = "block_number"
	EntriesFieldName     = "entries"
	Entries_IFieldName   = "entries.%d"
)

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
