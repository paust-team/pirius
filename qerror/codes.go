package qerror

type QErrCode uint32

const (
	//  00 - successful completion
	Success QErrCode = 0x0000

	// 01 - msg or field error
	ErrInvalidMsgType            = 0x0100
	ErrInvalidStartOffset        = 0x0101
	ErrMarshalAnyFailed          = 0x0102
	ErrUnmarshalAnyFailed        = 0x0103
	ErrMarshalFailed             = 0x0104
	ErrUnmarshalFailed           = 0x0105
	ErrTopicNotSet               = 0x0106
	ErrValidation                = 0x0107
	ErrTopicFragmentOffsetNotSet = 0x0108

	// 02 - coordinating error
	ErrCoordConnection          = 0x0200
	ErrCoordTargetAlreadyExists = 0x0201
	ErrCoordOperate             = 0x0202
	ErrCoordLockFail            = 0x0203
	ErrCoordEncodeFail          = 0x0204
	ErrCoordDecodeFail          = 0x0205
	ErrCoordNothingToRemove     = 0x0206
	ErrCoordRequest             = 0x0207
	ErrCoordNoNode              = 0x0208

	// 03 - rocksdb related error
	ErrDBOperate = 0x0300

	// 04 - network related error
	ErrNotConnected     = 0x0400
	ErrAlreadyConnected = 0x0401
	ErrDialFailed       = 0x0402

	// 05 - config related error
	ErrConfigValueNotSet = 0x0500

	// 10 - bootstrapping error
	ErrTopicNotExists = 0x1000
)
