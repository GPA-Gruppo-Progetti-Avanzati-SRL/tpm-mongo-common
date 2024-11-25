package checkpoint

type ResumeTokenCheckpointSvc interface {
	Retrieve(string) (ResumeToken, error)
	Store(tokenId string, token ResumeToken) error
}
