package job

// This file contains the paths to the field in the generated entity.
// A path is a string with all the identifiers from the root document to the single leaves.
// In case of maps and arrays place holder for the key (%s) or the index %d have been provided.

// @tpm-schematics:start-region("top-file-section")
// @tpm-schematics:end-region("top-file-section")

const (
	BidFieldName        = "_bid"
	EtFieldName         = "_et"
	AmbitFieldName      = "ambit"
	StatusFieldName     = "status"
	DueDateFieldName    = "due_date"
	PropertiesFieldName = "properties"
	TasksFieldName      = "tasks"
	Tasks_IFieldName    = "tasks.%d"
)

// @tpm-schematics:start-region("bottom-file-section")

const (
	Tasks_IFieldName_status = "tasks.%d.status"
)

// @tpm-schematics:end-region("bottom-file-section")
