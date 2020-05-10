package message

// Publisher is the message manager
type Publisher interface {
	// Pub sends a message on a particular topic
	Pub(interface{})
}
