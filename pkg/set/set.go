package set

// Set is not safe for concurrent writes.
// Use New or make before calling Add; other operations accept a nil Set.
type Set[T comparable] map[T]struct{}

func New[T comparable](values ...T) Set[T] {
	s := make(Set[T], len(values))
	for _, value := range values {
		s.Add(value)
	}

	return s
}

func (s Set[T]) Add(value T) { s[value] = struct{}{} }

func (s Set[T]) Remove(value T) { delete(s, value) }

func (s Set[T]) Contains(value T) bool {
	_, ok := s[value]
	return ok
}
