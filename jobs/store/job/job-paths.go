package job

// This file contains the paths to the field in the generated entity.
// A path is a string with all the identifiers from the root document to the single leaves.
// In case of maps and arrays place holder for the key (%s) or the index %d have been provided.

// @tpm-schematics:start-region("top-file-section")
// @tpm-schematics:end-region("top-file-section")

const (
	OIdFieldName                = "_id"
	DomainFieldName             = "domain"
	SiteFieldName               = "site"
	BidFieldName                = "_bid"
	EtFieldName                 = "_et"
	GroupFieldName              = "group"
	NameFieldName               = "name"
	StatusFieldName             = "status"
	DueDateFieldName            = "due_date"
	PropertiesFieldName         = "properties"
	TasksFieldName              = "tasks"
	Tasks_IFieldName            = "tasks.%d"
	SysInfoFieldName            = "sys_info"
	SysInfo_DoneAtFieldName     = "sys_info.done_at"
	SysInfo_CreatedAtFieldName  = "sys_info.created_at"
	SysInfo_ModifiedAtFieldName = "sys_info.modified_at"
)

// @tpm-schematics:start-region("bottom-file-section")

const (
	Tasks_IFieldName_status = "tasks.%d.status"
)

// @tpm-schematics:end-region("bottom-file-section")
