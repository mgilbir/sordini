package sordini

// Callback is the function that is called when a message is received.
type Callback func(topic string, offset int64, partition int32, key []byte, msg []byte)
