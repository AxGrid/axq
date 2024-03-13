package domain

type DBInfo struct {
	Version        string
	VersionComment string
	BufferSize     uint64
	BinLogSize     uint64
	FlushMethod    string
	Host           string
	Port           int
	DBName         string
}

type DBParams struct {
	User     string
	Password string
	Host     string
	Port     int
	DBName   string
}
