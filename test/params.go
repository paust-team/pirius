package test

type Params struct {
	values map[string]interface{}
}

func NewTestParams() Params {
	return Params{values: make(map[string]interface{})}
}

func (p Params) Set(key string, value any) {
	p.values[key] = value
}

func (p Params) Get(key string) any {
	return p.values[key]
}

func (p Params) GetString(key string) string {
	return p.values[key].(string)
}

func (p Params) GetBytes(key string) []byte {
	return p.values[key].([]byte)
}

func (p Params) GetBytesList(key string) [][]byte {
	return p.values[key].([][]byte)
}

func (p Params) GetInt(key string) int {
	return p.values[key].(int)
}

func (p Params) GetUint(key string) uint {
	return p.values[key].(uint)
}

func (p Params) GetUint32(key string) uint32 {
	return p.values[key].(uint32)
}

func (p Params) GetUint64(key string) uint64 {
	return p.values[key].(uint64)
}

func (p Params) Clear() {
	p.values = make(map[string]interface{})
}
