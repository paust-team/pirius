package pqerror

type PQCode uint32

const (
	//  00 - successful completion
	Success PQCode = 0x0000

	// 01 - msg or field error
	ErrInvalidMsgType     = 0x0100
	ErrInvalidStartOffset = 0x0101

	// 02 - zookeeper related error
	ErrZKConnection = 0x0200
	ErrZKTargetAlreadyExists = 0x0201
	ErrZKOperate             = 0x0202
	ErrTopicBrokersNotExist  = 0x0203
	ErrZKLockFail            = 0x0204
	ErrZKEncodeFail          = 0x0205
	ErrZKDecodeFail          = 0x0206
	ErrZKNothingToRemove     = 0x0207

	// 03 - rocksdb related error
	ErrDBOperate = 0x0300

	// 10 - broker internal error
	ErrInternal = 0x1000
)
