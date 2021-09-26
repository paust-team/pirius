package pqerror

type PQCode uint32

const (
	//  00 - successful completion
	Success PQCode = 0x0000

	// 01 - msg or field error
	ErrInvalidMsgType     = 0x0100
	ErrInvalidStartOffset = 0x0101
	ErrMarshalAnyFailed   = 0x0102
	ErrUnmarshalAnyFailed = 0x0103
	ErrMarshalFailed      = 0x0104
	ErrUnmarshalFailed    = 0x0105
	ErrTopicNotSet        = 0x0106
	ErrInvalidNodeId      = 0x0107

	// 02 - zookeeper related error
	ErrZKConnection          = 0x0200
	ErrZKTargetAlreadyExists = 0x0201
	ErrZKOperate             = 0x0202
	ErrTopicBrokersNotExist  = 0x0203
	ErrZKLockFail            = 0x0204
	ErrZKEncodeFail          = 0x0205
	ErrZKDecodeFail          = 0x0206
	ErrZKNothingToRemove     = 0x0207
	ErrZKRequest             = 0x0208

	// 03 - rocksdb related error
	ErrDBOperate = 0x0300

	// 04 - network related error
	ErrNotConnected     = 0x0400
	ErrAlreadyConnected = 0x0401
	ErrDialFailed       = 0x0402

	// 10 - broker internal error
	ErrInternal               = 0x1000
	ErrSocketClosed           = 0x1001
	ErrSocketRead             = 0x1002
	ErrSocketWrite            = 0x1003
	ErrWriteTimeOut           = 0x1004
	ErrReadTimeOut            = 0x1005
	ErrNotEnoughBuffer        = 0x1006
	ErrInvalidChecksum        = 0x1007
	ErrTopicNotExist          = 0x1008
	ErrStateTransition        = 0x1009
	ErrInvalidCaseFnCount     = 0x1010
	ErrPipeBuildFail          = 0x1011
	ErrInvalidPipeType        = 0x1012
	ErrInvalidMsgTypeToUnpack = 0x1013
	ErrTopicBrokerNotExist    = 0x1014
)
