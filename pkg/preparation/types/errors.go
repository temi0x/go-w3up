package types

import (
	"fmt"
)

type ErrEmpty struct {
	Field string
}

func (e ErrEmpty) Error() string {
	return fmt.Sprintf("%s cannot be empty", e.Field)
}
