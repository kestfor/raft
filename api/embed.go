// Package api embeds the client API contract.
package api

import _ "embed"

//go:embed client_openapi.json
var ClientOpenAPI string
