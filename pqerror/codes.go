package pqerror

type PQCode uint32

const (
	//  00 - successful completion
	Success PQCode = 0x0000

	// 01 - msg or field error
	ErrInvalidMsg         = 0x0100
	ErrInvalidStartOffset = 0x0101

	// 02 - zookeeper related error
	ErrZKConnectionFail = 0x0200
	ErrZKTargetAlreadyExists = 0x0201
	ErrZKOperate = 0x0202

	// 03 - rocksdb related error
	ErrDBOperate = 0x0300

	// 10 - broker internal error
	ErrInternal = 0x1000
)
