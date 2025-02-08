package checkpoint

type ResumeTokenCheckpointSvc interface {
	Retrieve(string) (ResumeToken, error)
	Store(tokenId string, token ResumeToken) error
	Synch(watcherId string, token ResumeToken) error
	Clear(tokenId string) error
	OnHistoryLost(tokenId string) error
}
