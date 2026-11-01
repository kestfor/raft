// Package httpapi embeds the public HTTP contract in the server binary.
package httpapi

import _ "embed"

//go:embed openapi.yaml
var OpenAPI string
