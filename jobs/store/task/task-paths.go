package task

// This file contains the paths to the field in the generated entity.
// A path is a string with all the identifiers from the root document to the single leaves.
// In case of maps and arrays place holder for the key (%s) or the index %d have been provided.

// @tpm-schematics:start-region("top-file-section")
// @tpm-schematics:end-region("top-file-section")

const (
	OIdFieldName                             = "_id"
	DomainFieldName                          = "domain"
	SiteFieldName                            = "site"
	BidFieldName                             = "_bid"
	EtFieldName                              = "_et"
	StatusFieldName                          = "status"
	GroupFieldName                           = "group"
	NameFieldName                            = "name"
	JobIdFieldName                           = "job_id"
	PropertiesFieldName                      = "properties"
	PartitionsFieldName                      = "partitions"
	Partitions_IFieldName                    = "partitions.%d"
	SysInfoFieldName                         = "sys_info"
	Partitions_I_SysInfo_DoneAtFieldName     = "partitions.%d.sys_info.done_at"
	Partitions_SysInfo_DoneAtFieldName       = "partitions.sys_info.done_at"
	SysInfo_DoneAtFieldName                  = "sys_info.done_at"
	Partitions_I_SysInfo_CreatedAtFieldName  = "partitions.%d.sys_info.created_at"
	Partitions_SysInfo_CreatedAtFieldName    = "partitions.sys_info.created_at"
	SysInfo_CreatedAtFieldName               = "sys_info.created_at"
	Partitions_I_SysInfo_ModifiedAtFieldName = "partitions.%d.sys_info.modified_at"
	Partitions_SysInfo_ModifiedAtFieldName   = "partitions.sys_info.modified_at"
	SysInfo_ModifiedAtFieldName              = "sys_info.modified_at"
)

// @tpm-schematics:start-region("bottom-file-section")

const (
	PartitionsIStatusFieldName       = "partitions.%d.status"
	PartitionsIErrorsFieldName       = "partitions.%d.errors"
	PartitionsIAcquisitionsFieldName = "partitions.%d.acquisitions"
)

// @tpm-schematics:end-region("bottom-file-section")
