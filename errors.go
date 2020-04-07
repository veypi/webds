package webds

import "errors"

var (
	ErrDuplicatedClient = errors.New("duplicated client")
	ErrID               = errors.New("id is not exist")
	ErrOrigin           = errors.New("error origin")
)
