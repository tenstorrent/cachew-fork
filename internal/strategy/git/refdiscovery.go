package git

import (
	"net/http"
	"slices"
	"strings"

	"github.com/alecthomas/errors"
)

// refDiscovery is what a smart-HTTP request needs from the mirror's refs.
type refDiscovery int

const (
	// refDiscoveryNone resolves no refs by name: a fetch by object ID, or a
	// request that is not upload-pack at all.
	refDiscoveryNone refDiscovery = iota
	// refDiscoveryCapabilities is a protocol v2 info/refs, which advertises
	// capabilities only. The refs a v2 client needs come later, from ls-refs.
	refDiscoveryCapabilities
	// refDiscoveryRefs lists refs: a protocol v0 info/refs advertisement, or a
	// v2 ls-refs command.
	refDiscoveryRefs
)

// classifyRefDiscovery says what the request needs from the mirror's refs and,
// for an ls-refs command, the ref-prefix arguments that limit which refs it
// lists. A body that cannot be parsed is treated as listing every ref.
func classifyRefDiscovery(r *http.Request, pathValue string, isInfoRefs bool) (refDiscovery, []string, error) {
	if isInfoRefs {
		if isProtocolV2(r) {
			return refDiscoveryCapabilities, nil, nil
		}
		return refDiscoveryRefs, nil, nil
	}
	isLsRefs, err := requestIsLsRefs(pathValue, r)
	if err != nil {
		return refDiscoveryNone, nil, errors.Wrap(err, "inspect ls-refs request")
	}
	if !isLsRefs {
		return refDiscoveryNone, nil, nil
	}
	body, err := bufferRequestBody(r)
	if err != nil {
		return refDiscoveryNone, nil, errors.Wrap(err, "buffer ls-refs request")
	}
	req, err := ParseUploadPackRequest(body, strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip"))
	if err != nil {
		return refDiscoveryRefs, nil, nil //nolint:nilerr // an unparseable ls-refs still gets the whole-repo check
	}
	return refDiscoveryRefs, req.RefPrefixes, nil
}

// isProtocolV2 reports whether the client asked for git wire protocol v2, whose
// colon-separated parameters arrive in the Git-Protocol header.
func isProtocolV2(r *http.Request) bool {
	return slices.Contains(strings.Split(r.Header.Get("Git-Protocol"), ":"), "version=2")
}
