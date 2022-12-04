package xorm

func (session *Session) Upsert(beans ...interface{}) (int64, error) {
	return session.replaceOrUpsert("UPSERT", beans...)
}
