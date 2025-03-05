package util

import (
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"regexp"
	"time"
)

// List of codes from https://www.mongodb.com/docs/manual/reference/error-codes/ retrieved on 2024-12-15 11:13:10

const (
	MongoErrInternalError                                               int32 = 1
	MongoErrBadValue                                                    int32 = 2
	MongoErrNoSuchKey                                                   int32 = 4
	MongoErrGraphContainsCycle                                          int32 = 5
	MongoErrHostUnreachable                                             int32 = 6
	MongoErrHostNotFound                                                int32 = 7
	MongoErrUnknownError                                                int32 = 8
	MongoErrFailedToParse                                               int32 = 9
	MongoErrCannotMutateObject                                          int32 = 10
	MongoErrUserNotFound                                                int32 = 11
	MongoErrUnsupportedFormat                                           int32 = 12
	MongoErrUnauthorized                                                int32 = 13
	MongoErrTypeMismatch                                                int32 = 14
	MongoErrOverflow                                                    int32 = 15
	MongoErrInvalidLength                                               int32 = 16
	MongoErrProtocolError                                               int32 = 17
	MongoErrAuthenticationFailed                                        int32 = 18
	MongoErrCannotReuseObject                                           int32 = 19
	MongoErrIllegalOperation                                            int32 = 20
	MongoErrEmptyArrayOperation                                         int32 = 21
	MongoErrInvalidBSON                                                 int32 = 22
	MongoErrAlreadyInitialized                                          int32 = 23
	MongoErrLockTimeout                                                 int32 = 24
	MongoErrRemoteValidationError                                       int32 = 25
	MongoErrNamespaceNotFound                                           int32 = 26
	MongoErrIndexNotFound                                               int32 = 27
	MongoErrPathNotViable                                               int32 = 28
	MongoErrNonExistentPath                                             int32 = 29
	MongoErrInvalidPath                                                 int32 = 30
	MongoErrRoleNotFound                                                int32 = 31
	MongoErrRolesNotRelated                                             int32 = 32
	MongoErrPrivilegeNotFound                                           int32 = 33
	MongoErrCannotBackfillArray                                         int32 = 34
	MongoErrUserModificationFailed                                      int32 = 35
	MongoErrRemoteChangeDetected                                        int32 = 36
	MongoErrFileRenameFailed                                            int32 = 37
	MongoErrFileNotOpen                                                 int32 = 38
	MongoErrFileStreamFailed                                            int32 = 39
	MongoErrConflictingUpdateOperators                                  int32 = 40
	MongoErrFileAlreadyOpen                                             int32 = 41
	MongoErrLogWriteFailed                                              int32 = 42
	MongoErrCursorNotFound                                              int32 = 43
	MongoErrUserDataInconsistent                                        int32 = 45
	MongoErrLockBusy                                                    int32 = 46
	MongoErrNoMatchingDocument                                          int32 = 47
	MongoErrNamespaceExists                                             int32 = 48
	MongoErrInvalidRoleModification                                     int32 = 49
	MongoErrMaxTimeMSExpired                                            int32 = 50
	MongoErrManualInterventionRequired                                  int32 = 51
	MongoErrDollarPrefixedFieldName                                     int32 = 52
	MongoErrInvalidIdField                                              int32 = 53
	MongoErrNotSingleValueField                                         int32 = 54
	MongoErrInvalidDBRef                                                int32 = 55
	MongoErrEmptyFieldName                                              int32 = 56
	MongoErrDottedFieldName                                             int32 = 57
	MongoErrRoleModificationFailed                                      int32 = 58
	MongoErrCommandNotFound                                             int32 = 59
	MongoErrShardKeyNotFound                                            int32 = 61
	MongoErrOplogOperationUnsupported                                   int32 = 62
	MongoErrStaleShardVersion                                           int32 = 63
	MongoErrWriteConcernFailed                                          int32 = 64
	MongoErrMultipleErrorsOccurred                                      int32 = 65
	MongoErrImmutableField                                              int32 = 66
	MongoErrCannotCreateIndex                                           int32 = 67
	MongoErrIndexAlreadyExists                                          int32 = 68
	MongoErrAuthSchemaIncompatible                                      int32 = 69
	MongoErrShardNotFound                                               int32 = 70
	MongoErrReplicaSetNotFound                                          int32 = 71
	MongoErrInvalidOptions                                              int32 = 72
	MongoErrInvalidNamespace                                            int32 = 73
	MongoErrNodeNotFound                                                int32 = 74
	MongoErrWriteConcernLegacyOK                                        int32 = 75
	MongoErrNoReplicationEnabled                                        int32 = 76
	MongoErrOperationIncomplete                                         int32 = 77
	MongoErrCommandResultSchemaViolation                                int32 = 78
	MongoErrUnknownReplWriteConcern                                     int32 = 79
	MongoErrRoleDataInconsistent                                        int32 = 80
	MongoErrNoMatchParseContext                                         int32 = 81
	MongoErrNoProgressMade                                              int32 = 82
	MongoErrRemoteResultsUnavailable                                    int32 = 83
	MongoErrIndexOptionsConflict                                        int32 = 85
	MongoErrIndexKeySpecsConflict                                       int32 = 86
	MongoErrCannotSplit                                                 int32 = 87
	MongoErrNetworkTimeout                                              int32 = 89
	MongoErrCallbackCanceled                                            int32 = 90
	MongoErrShutdownInProgress                                          int32 = 91
	MongoErrSecondaryAheadOfPrimary                                     int32 = 92
	MongoErrInvalidReplicaSetConfig                                     int32 = 93
	MongoErrNotYetInitialized                                           int32 = 94
	MongoErrNotSecondary                                                int32 = 95
	MongoErrOperationFailed                                             int32 = 96
	MongoErrNoProjectionFound                                           int32 = 97
	MongoErrDBPathInUse                                                 int32 = 98
	MongoErrUnsatisfiableWriteConcern                                   int32 = 100
	MongoErrOutdatedClient                                              int32 = 101
	MongoErrIncompatibleAuditMetadata                                   int32 = 102
	MongoErrNewReplicaSetConfigurationIncompatible                      int32 = 103
	MongoErrNodeNotElectable                                            int32 = 104
	MongoErrIncompatibleShardingMetadata                                int32 = 105
	MongoErrDistributedClockSkewed                                      int32 = 106
	MongoErrLockFailed                                                  int32 = 107
	MongoErrInconsistentReplicaSetNames                                 int32 = 108
	MongoErrConfigurationInProgress                                     int32 = 109
	MongoErrCannotInitializeNodeWithData                                int32 = 110
	MongoErrNotExactValueField                                          int32 = 111
	MongoErrWriteConflict                                               int32 = 112
	MongoErrInitialSyncFailure                                          int32 = 113
	MongoErrInitialSyncOplogSourceMissing                               int32 = 114
	MongoErrCommandNotSupported                                         int32 = 115
	MongoErrDocTooLargeForCapped                                        int32 = 116
	MongoErrConflictingOperationInProgress                              int32 = 117
	MongoErrNamespaceNotSharded                                         int32 = 118
	MongoErrInvalidSyncSource                                           int32 = 119
	MongoErrOplogStartMissing                                           int32 = 120
	MongoErrDocumentValidationFailure                                   int32 = 121
	MongoErrNotAReplicaSet                                              int32 = 123
	MongoErrIncompatibleElectionProtocol                                int32 = 124
	MongoErrCommandFailed                                               int32 = 125
	MongoErrRPCProtocolNegotiationFailed                                int32 = 126
	MongoErrUnrecoverableRollbackError                                  int32 = 127
	MongoErrLockNotFound                                                int32 = 128
	MongoErrLockStateChangeFailed                                       int32 = 129
	MongoErrSymbolNotFound                                              int32 = 130
	MongoErrFailedToSatisfyReadPreference                               int32 = 133
	MongoErrReadConcernMajorityNotAvailableYet                          int32 = 134
	MongoErrStaleTerm                                                   int32 = 135
	MongoErrCappedPositionLost                                          int32 = 136
	MongoErrIncompatibleShardingConfigVersion                           int32 = 137
	MongoErrRemoteOplogStale                                            int32 = 138
	MongoErrJSInterpreterFailure                                        int32 = 139
	MongoErrInvalidSSLConfiguration                                     int32 = 140
	MongoErrSSLHandshakeFailed                                          int32 = 141
	MongoErrJSUncatchableError                                          int32 = 142
	MongoErrCursorInUse                                                 int32 = 143
	MongoErrIncompatibleCatalogManager                                  int32 = 144
	MongoErrPooledConnectionsDropped                                    int32 = 145
	MongoErrExceededMemoryLimit                                         int32 = 146
	MongoErrZLibError                                                   int32 = 147
	MongoErrReadConcernMajorityNotEnabled                               int32 = 148
	MongoErrNoConfigPrimary                                             int32 = 149
	MongoErrStaleEpoch                                                  int32 = 150
	MongoErrOperationCannotBeBatched                                    int32 = 151
	MongoErrOplogOutOfOrder                                             int32 = 152
	MongoErrChunkTooBig                                                 int32 = 153
	MongoErrInconsistentShardIdentity                                   int32 = 154
	MongoErrCannotApplyOplogWhilePrimary                                int32 = 155
	MongoErrCanRepairToDowngrade                                        int32 = 157
	MongoErrMustUpgrade                                                 int32 = 158
	MongoErrDurationOverflow                                            int32 = 159
	MongoErrMaxStalenessOutOfRange                                      int32 = 160
	MongoErrIncompatibleCollationVersion                                int32 = 161
	MongoErrCollectionIsEmpty                                           int32 = 162
	MongoErrZoneStillInUse                                              int32 = 163
	MongoErrInitialSyncActive                                           int32 = 164
	MongoErrViewDepthLimitExceeded                                      int32 = 165
	MongoErrCommandNotSupportedOnView                                   int32 = 166
	MongoErrOptionNotSupportedOnView                                    int32 = 167
	MongoErrInvalidPipelineOperator                                     int32 = 168
	MongoErrCommandOnShardedViewNotSupportedOnMongod                    int32 = 169
	MongoErrTooManyMatchingDocuments                                    int32 = 170
	MongoErrCannotIndexParallelArrays                                   int32 = 171
	MongoErrTransportSessionClosed                                      int32 = 172
	MongoErrTransportSessionNotFound                                    int32 = 173
	MongoErrTransportSessionUnknown                                     int32 = 174
	MongoErrQueryPlanKilled                                             int32 = 175
	MongoErrFileOpenFailed                                              int32 = 176
	MongoErrZoneNotFound                                                int32 = 177
	MongoErrRangeOverlapConflict                                        int32 = 178
	MongoErrWindowsPdhError                                             int32 = 179
	MongoErrBadPerfCounterPath                                          int32 = 180
	MongoErrAmbiguousIndexKeyPattern                                    int32 = 181
	MongoErrInvalidViewDefinition                                       int32 = 182
	MongoErrClientMetadataMissingField                                  int32 = 183
	MongoErrClientMetadataAppNameTooLarge                               int32 = 184
	MongoErrClientMetadataDocumentTooLarge                              int32 = 185
	MongoErrClientMetadataCannotBeMutated                               int32 = 186
	MongoErrLinearizableReadConcernError                                int32 = 187
	MongoErrIncompatibleServerVersion                                   int32 = 188
	MongoErrPrimarySteppedDown                                          int32 = 189
	MongoErrMasterSlaveConnectionFailure                                int32 = 190
	MongoErrFailPointEnabled                                            int32 = 192
	MongoErrNoShardingEnabled                                           int32 = 193
	MongoErrBalancerInterrupted                                         int32 = 194
	MongoErrViewPipelineMaxSizeExceeded                                 int32 = 195
	MongoErrInvalidIndexSpecificationOption                             int32 = 197
	MongoErrReplicaSetMonitorRemoved                                    int32 = 199
	MongoErrChunkRangeCleanupPending                                    int32 = 200
	MongoErrCannotBuildIndexKeys                                        int32 = 201
	MongoErrNetworkInterfaceExceededTimeLimit                           int32 = 202
	MongoErrShardingStateNotInitialized                                 int32 = 203
	MongoErrTimeProofMismatch                                           int32 = 204
	MongoErrClusterTimeFailsRateLimiter                                 int32 = 205
	MongoErrNoSuchSession                                               int32 = 206
	MongoErrInvalidUUID                                                 int32 = 207
	MongoErrTooManyLocks                                                int32 = 208
	MongoErrStaleClusterTime                                            int32 = 209
	MongoErrCannotVerifyAndSignLogicalTime                              int32 = 210
	MongoErrKeyNotFound                                                 int32 = 211
	MongoErrIncompatibleRollbackAlgorithm                               int32 = 212
	MongoErrDuplicateSession                                            int32 = 213
	MongoErrAuthenticationRestrictionUnmet                              int32 = 214
	MongoErrDatabaseDropPending                                         int32 = 215
	MongoErrElectionInProgress                                          int32 = 216
	MongoErrIncompleteTransactionHistory                                int32 = 217
	MongoErrUpdateOperationFailed                                       int32 = 218
	MongoErrFTDCPathNotSet                                              int32 = 219
	MongoErrFTDCPathAlreadySet                                          int32 = 220
	MongoErrIndexModified                                               int32 = 221
	MongoErrCloseChangeStream                                           int32 = 222
	MongoErrIllegalOpMsgFlag                                            int32 = 223
	MongoErrQueryFeatureNotAllowed                                      int32 = 224
	MongoErrTransactionTooOld                                           int32 = 225
	MongoErrAtomicityFailure                                            int32 = 226
	MongoErrCannotImplicitlyCreateCollection                            int32 = 227
	MongoErrSessionTransferIncomplete                                   int32 = 228
	MongoErrMustDowngrade                                               int32 = 229
	MongoErrDNSHostNotFound                                             int32 = 230
	MongoErrDNSProtocolError                                            int32 = 231
	MongoErrMaxSubPipelineDepthExceeded                                 int32 = 232
	MongoErrTooManyDocumentSequences                                    int32 = 233
	MongoErrRetryChangeStream                                           int32 = 234
	MongoErrInternalErrorNotSupported                                   int32 = 235
	MongoErrForTestingErrorExtraInfo                                    int32 = 236
	MongoErrCursorKilled                                                int32 = 237
	MongoErrNotImplemented                                              int32 = 238
	MongoErrSnapshotTooOld                                              int32 = 239
	MongoErrDNSRecordTypeMismatch                                       int32 = 240
	MongoErrConversionFailure                                           int32 = 241
	MongoErrCannotCreateCollection                                      int32 = 242
	MongoErrIncompatibleWithUpgradedServer                              int32 = 243
	MongoErrBrokenPromise                                               int32 = 245
	MongoErrSnapshotUnavailable                                         int32 = 246
	MongoErrProducerConsumerQueueBatchTooLarge                          int32 = 247
	MongoErrProducerConsumerQueueEndClosed                              int32 = 248
	MongoErrStaleDbVersion                                              int32 = 249
	MongoErrStaleChunkHistory                                           int32 = 250
	MongoErrNoSuchTransaction                                           int32 = 251
	MongoErrReentrancyNotAllowed                                        int32 = 252
	MongoErrFreeMonHttpInFlight                                         int32 = 253
	MongoErrFreeMonHttpTemporaryFailure                                 int32 = 254
	MongoErrFreeMonHttpPermanentFailure                                 int32 = 255
	MongoErrTransactionCommitted                                        int32 = 256
	MongoErrTransactionTooLarge                                         int32 = 257
	MongoErrUnknownFeatureCompatibilityVersion                          int32 = 258
	MongoErrKeyedExecutorRetry                                          int32 = 259
	MongoErrInvalidResumeToken                                          int32 = 260
	MongoErrTooManyLogicalSessions                                      int32 = 261
	MongoErrExceededTimeLimit                                           int32 = 262
	MongoErrOperationNotSupportedInTransaction                          int32 = 263
	MongoErrTooManyFilesOpen                                            int32 = 264
	MongoErrOrphanedRangeCleanUpFailed                                  int32 = 265
	MongoErrFailPointSetFailed                                          int32 = 266
	MongoErrPreparedTransactionInProgress                               int32 = 267
	MongoErrCannotBackup                                                int32 = 268
	MongoErrDataModifiedByRepair                                        int32 = 269
	MongoErrRepairedReplicaSetNode                                      int32 = 270
	MongoErrJSInterpreterFailureWithStack                               int32 = 271
	MongoErrMigrationConflict                                           int32 = 272
	MongoErrProducerConsumerQueueProducerQueueDepthExceeded             int32 = 273
	MongoErrProducerConsumerQueueConsumed                               int32 = 274
	MongoErrExchangePassthrough                                         int32 = 275
	MongoErrIndexBuildAborted                                           int32 = 276
	MongoErrAlarmAlreadyFulfilled                                       int32 = 277
	MongoErrUnsatisfiableCommitQuorum                                   int32 = 278
	MongoErrClientDisconnect                                            int32 = 279
	MongoErrChangeStreamFatalError                                      int32 = 280
	MongoErrTransactionCoordinatorSteppingDown                          int32 = 281
	MongoErrTransactionCoordinatorReachedAbortDecision                  int32 = 282
	MongoErrWouldChangeOwningShard                                      int32 = 283
	MongoErrForTestingErrorExtraInfoWithExtraInfoInNamespace            int32 = 284
	MongoErrIndexBuildAlreadyInProgress                                 int32 = 285
	MongoErrChangeStreamHistoryLost                                     int32 = 286
	MongoErrTransactionCoordinatorDeadlineTaskCanceled                  int32 = 287
	MongoErrChecksumMismatch                                            int32 = 288
	MongoErrWaitForMajorityServiceEarlierOpTimeAvailable                int32 = 289
	MongoErrTransactionExceededLifetimeLimitSeconds                     int32 = 290
	MongoErrNoQueryExecutionPlans                                       int32 = 291
	MongoErrQueryExceededMemoryLimitNoDiskUseAllowed                    int32 = 292
	MongoErrInvalidSeedList                                             int32 = 293
	MongoErrInvalidTopologyType                                         int32 = 294
	MongoErrInvalidHeartBeatFrequency                                   int32 = 295
	MongoErrTopologySetNameRequired                                     int32 = 296
	MongoErrHierarchicalAcquisitionLevelViolation                       int32 = 297
	MongoErrInvalidServerType                                           int32 = 298
	MongoErrOCSPCertificateStatusRevoked                                int32 = 299
	MongoErrRangeDeletionAbandonedBecauseCollectionWithUUIDDoesNotExist int32 = 300
	MongoErrDataCorruptionDetected                                      int32 = 301
	MongoErrOCSPCertificateStatusUnknown                                int32 = 302
	MongoErrSplitHorizonChange                                          int32 = 303
	MongoErrShardInvalidatedForTargeting                                int32 = 304
	MongoErrRangeDeletionAbandonedBecauseTaskDocumentDoesNotExist       int32 = 307
	MongoErrCurrentConfigNotCommittedYet                                int32 = 308
	MongoErrExhaustCommandFinished                                      int32 = 309
	MongoErrPeriodicJobIsStopped                                        int32 = 310
	MongoErrTransactionCoordinatorCanceled                              int32 = 311
	MongoErrOperationIsKilledAndDelisted                                int32 = 312
	MongoErrResumableRangeDeleterDisabled                               int32 = 313
	MongoErrObjectIsBusy                                                int32 = 314
	MongoErrTooStaleToSyncFromSource                                    int32 = 315
	MongoErrQueryTrialRunCompleted                                      int32 = 316
	MongoErrConnectionPoolExpired                                       int32 = 317
	MongoErrForTestingOptionalErrorExtraInfo                            int32 = 318
	MongoErrMovePrimaryInProgress                                       int32 = 319
	MongoErrTenantMigrationConflict                                     int32 = 320
	MongoErrTenantMigrationCommitted                                    int32 = 321
	MongoErrAPIVersionError                                             int32 = 322
	MongoErrAPIStrictError                                              int32 = 323
	MongoErrAPIDeprecationError                                         int32 = 324
	MongoErrTenantMigrationAborted                                      int32 = 325
	MongoErrOplogQueryMinTsMissing                                      int32 = 326
	MongoErrNoSuchTenantMigration                                       int32 = 327
	MongoErrTenantMigrationAccessBlockerShuttingDown                    int32 = 328
	MongoErrTenantMigrationInProgress                                   int32 = 329
	MongoErrSkipCommandExecution                                        int32 = 330
	MongoErrFailedToRunWithReplyBuilder                                 int32 = 331
	MongoErrCannotDowngrade                                             int32 = 332
	MongoErrServiceExecutorInShutdown                                   int32 = 333
	MongoErrMechanismUnavailable                                        int32 = 334
	MongoErrTenantMigrationForgotten                                    int32 = 335
	MongoErrSocketException                                             int32 = 9001
	MongoErrCannotGrowDocumentInCappedNamespace                         int32 = 10003
	MongoErrNotWritablePrimary                                          int32 = 10107
	MongoErrBSONObjectTooLarge                                          int32 = 10334
	MongoErrDuplicateKey                                                int32 = 11000
	MongoErrInterruptedAtShutdown                                       int32 = 11600
	MongoErrInterrupted                                                 int32 = 11601
	MongoErrInterruptedDueToReplStateChange                             int32 = 11602
	MongoErrBackgroundOperationInProgressForDatabase                    int32 = 12586
	MongoErrBackgroundOperationInProgressForNamespace                   int32 = 12587
	MongoErrMergeStageNoMatchingDocument                                int32 = 13113
	MongoErrDatabaseDifferCase                                          int32 = 13297
	MongoErrStaleConfig                                                 int32 = 13388
	MongoErrNotPrimaryNoSecondaryOk                                     int32 = 13435
	MongoErrNotPrimaryOrSecondary                                       int32 = 13436
	MongoErrOutOfDiskSpace                                              int32 = 14031
	MongoErrClientMarkedKilled                                          int32 = 46841
)

func MongoError(err error, mongoDbVersion MongoDbVersion) (int32, mongo.CommandError) {
	const semLogContext = "mongo::error"

	var mongoErr mongo.CommandError
	switch {
	case errors.As(err, &mongoErr):
		log.Error().Err(mongoErr).Interface("server-version", mongoDbVersion).Int32("error-code", mongoErr.Code).Str("error-name", mongoErr.Name).Str("error-msg", mongoErr.Message).Msg(semLogContext + " - mongo.CommandError")
		code := CommandErrorCode(mongoErr, mongoDbVersion)
		return code, mongoErr
	default:
		log.Error().Err(err).Interface("server-version", mongoDbVersion).Str("error-type", fmt.Sprintf("%T", err)).Msg(semLogContext + " - !mongo.CommandError")
	}

	return -1, mongo.CommandError{}
}

var Version4ResumeTokenExtractionRegexp = regexp.MustCompile("{_data: \\\"([A-Z0-9]*)\\\"}")

func CommandErrorCode(err mongo.CommandError, mongoDbVersion MongoDbVersion) int32 {
	const semLogContext = "mongo::error-command"
	code := err.Code
	switch {
	case code == MongoErrChangeStreamFatalError && mongoDbVersion.IsVersion4():
		code = MongoErrChangeStreamHistoryLost
		data := util.ExtractCapturedGroupIfMatch(Version4ResumeTokenExtractionRegexp, err.Message)
		if data != "" {
			rt := checkpoint.ResumeToken{
				Value: data,
				At:    time.Now().Format(time.RFC3339Nano),
			}
			rti, err := rt.Parse()
			if err == nil {
				log.Error().Interface("resume-token-info", rti).Msg(semLogContext)
			} else {
				log.Error().Err(err).Msg(semLogContext)
			}
		}
	default:

	}

	return code
}
