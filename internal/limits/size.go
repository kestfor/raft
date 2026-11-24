// Package limits defines byte limits shared by the wire and journal formats.
package limits

import "math"

const (
	// The WAL length field is uint32. Its frame must also fit in a Go slice.
	WALHeader = 12
	WALRecord = min(math.MaxUint32, math.MaxInt-WALHeader)

	// Leave space for the protobuf envelope and the base64 WAL record metadata.
	GRPCMessage = math.MaxInt32
	envelope    = 64 << 10
	Command     = min(GRPCMessage-envelope, (WALRecord-envelope)/4*3)
)
