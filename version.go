package snodra

// Version is the version of this library.
const Version = "0.5.2"

// clientVersion is the identity the library announces to the hub.
func clientVersion() string { return "snodra-messaging-go/" + Version }
