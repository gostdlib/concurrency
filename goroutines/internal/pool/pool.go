package pool

// PoolType is for internal use. Please ignore.
type PoolType uint8

const (
	PTUnknown PoolType = 0
	PTPooled  PoolType = 1
	PTLimited PoolType = 2
)

// SubmitOptions is used internally. Please ignore.
type SubmitOptions struct {
	Type PoolType

	NonBlocking bool
}
