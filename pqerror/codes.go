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

	// 10 - broker internal error
	ErrInternal = 0x1000
)
