package utils

import (
	"encoding/json"
)

func DeepCopy[T any](input T) (T, error) {
	data, err := json.Marshal(input)
	if err != nil {
		return *new(T), err
	}

	var output T
	err = json.Unmarshal(data, &output)
	if err != nil {
		return *new(T), err
	}

	return output, nil
}
