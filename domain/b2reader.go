package domain

type B2Reader interface {
	Pop() Message
	C() <-chan Message
}
