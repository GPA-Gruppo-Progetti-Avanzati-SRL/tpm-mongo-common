package checkpoint

type ResumeTokenCheckpointSvc interface {
	Retrieve(string) (ResumeToken, error)
	Store(tokenId string, token ResumeToken) error
	StoreIdle(tokenId string, token ResumeToken) error
	ClearIdle()
	Synch(watcherId string, token ResumeToken) error
	Clear(tokenId string) error
	OnHistoryLost(tokenId string) error
}
