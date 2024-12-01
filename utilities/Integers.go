package utilities

import (
	"log"
	"strconv"
)

// ParseInt converts a string to an integer
func ParseInt(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		log.Fatal(err)
	}
	return i
}
