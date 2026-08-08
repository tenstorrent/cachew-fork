// Package authztest serves the smart-HTTP ref advertisements that upstream
// authorization probes accept, so tests standing in for a git host grant read
// access the same way a real one does.
package authztest

import "net/http"

// Advertisement is the smallest well-formed upload-pack ref advertisement: the
// service pkt-line every smart-HTTP response opens with, then a flush packet.
const Advertisement = "001e# service=git-upload-pack\n0000"

// ContentType is the media type a smart-HTTP server sends with Advertisement.
const ContentType = "application/x-git-upload-pack-advertisement"

// WriteAdvertisement responds as an upstream that serves the repository to the
// credentials on the request.
func WriteAdvertisement(w http.ResponseWriter) {
	w.Header().Set("Content-Type", ContentType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(Advertisement)) //nolint:errcheck // a test server has nowhere to report this
}
