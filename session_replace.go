package xorm

func (session *Session) Replace(beans ...interface{}) (int64, error) {
	return session.replaceOrUpsert("REPLACE", beans...)
}
