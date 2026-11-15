package sender

import "math/rand/v2"

type defaultRand struct{}

func (defaultRand) Float64() float64 {
	return rand.Float64()
}
