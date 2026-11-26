package snodra

// Version is the version of this library.
const Version = "0.4.0"

// clientVersion is the identity the library announces to the hub.
func clientVersion() string { return "snodra-messaging-go/" + Version }
