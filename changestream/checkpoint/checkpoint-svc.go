package checkpoint

type ResumeTokenCheckpointSvc interface {
	Retrieve(string) (ResumeToken, error)
	StoreIdle(tokenId string, token ResumeToken) error
	ClearIdle()
	CommitAt(watcherId string, token ResumeToken, syncRequired bool) error
	Clear(tokenId string) error
	OnHistoryLost(tokenId string) error
}
