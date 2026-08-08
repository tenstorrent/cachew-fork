package repoauthz_test

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/tenstorrent/internal-cachew/internal/repoauthz"
	"github.com/tenstorrent/internal-cachew/internal/repoauthz/authztest"
)

// advertisementPattern independently states what upstream must have sent for a
// probe to grant read access, so the fuzz target checks ProbeRefs against a
// second statement of the rule rather than against itself.
var advertisementPattern = regexp.MustCompile(`\A[0-9a-fA-F]{4}# service=git-upload-pack`)

// FuzzProbeRefsForwarding asserts the three properties the authorization
// decision rests on: the client's credentials reach upstream exactly as
// supplied, the probe upstream answers is the ref advertisement for the
// repository being authorized, and read access is granted only for a response
// that actually is such an advertisement. A 200 from another endpoint, or a 200
// that is not an advertisement, proves nothing about read access, so either one
// is as good as a bypass.
func FuzzProbeRefsForwarding(f *testing.F) {
	seeds := []struct {
		repoPath      string
		authorization string
		contentType   string
		body          string
	}{
		{"org/repo", "Bearer client-token", authztest.ContentType, authztest.Advertisement},
		{"org/repo.git", "Basic dXNlcjpwdw==", authztest.ContentType, authztest.Advertisement},
		{"org/repo", "", authztest.ContentType, authztest.Advertisement},
		{"org/repo", "Bearer  padded  ", authztest.ContentType, authztest.Advertisement},
		{"org/repo", "Bearer \x00", authztest.ContentType, authztest.Advertisement},
		{"org/repo", "Bearer x\r\nX-Injected: 1", authztest.ContentType, authztest.Advertisement},
		{"org/repo", strings.Repeat("A", 16384), authztest.ContentType, authztest.Advertisement},
		{"org/repo", "Bearer ü", authztest.ContentType, authztest.Advertisement},
		{"org/repo#fragment", "Bearer client-token", authztest.ContentType, authztest.Advertisement},
		{"org/repo?service=git-receive-pack", "Bearer client-token", authztest.ContentType, authztest.Advertisement},
		{"org/%2e%2e/private", "Bearer client-token", authztest.ContentType, authztest.Advertisement},
		{"org/repo\r\nX-Injected: 1", "Bearer client-token", authztest.ContentType, authztest.Advertisement},
		{"", "", authztest.ContentType, authztest.Advertisement},
		{"org/repo", "Bearer client-token", "text/html; charset=utf-8", "<html>sign in</html>"},
		{"org/repo", "Bearer client-token", "", authztest.Advertisement},
		{"org/repo", "Bearer client-token", authztest.ContentType, "<html>sign in</html>"},
		{"org/repo", "Bearer client-token", authztest.ContentType, ""},
		{"org/repo", "Bearer client-token", authztest.ContentType, "001e# service=git-upload-pac"},
		{"org/repo", "Bearer client-token", authztest.ContentType, "0021# service=git-receive-pack\n0000"},
		{"org/repo", "Bearer client-token", authztest.ContentType + "; charset=utf-8", authztest.Advertisement},
		{"org/repo", "Bearer client-token", "APPLICATION/X-GIT-UPLOAD-PACK-ADVERTISEMENT", authztest.Advertisement},
	}
	for _, seed := range seeds {
		f.Add(seed.repoPath, seed.authorization, seed.contentType, seed.body)
	}

	var (
		mu          sync.Mutex
		requests    int
		gotAuth     []string
		gotPath     string
		gotQuery    string
		contentType string
		body        string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		requests++
		gotAuth = r.Header.Values("Authorization")
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body)) //nolint:errcheck // a test server has nowhere to report this
	}))
	f.Cleanup(server.Close)

	f.Fuzz(func(t *testing.T, repoPath, authorization, responseContentType, responseBody string) {
		// A Content-Type that cannot be sent at all would fail the request for a
		// reason that says nothing about the probe.
		if !isSendableHeaderValue(responseContentType) {
			t.Skip()
		}

		mu.Lock()
		before := requests
		contentType = responseContentType
		body = responseBody
		mu.Unlock()

		result, err := repoauthz.ProbeRefs(t.Context(), server.Client(), server.URL+"/"+repoPath, authorization)

		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			if requests != before {
				t.Fatalf("probe reported failure %v after reaching upstream", err)
			}
			return
		}
		if result.Status != http.StatusOK || requests != before+1 {
			t.Fatalf("probe returned status %d after %d upstream requests", result.Status, requests-before)
		}
		// Header values are trimmed of optional whitespace by HTTP itself, on
		// this leg and the forwarded one alike.
		want := []string{}
		if trimmed := strings.Trim(authorization, " \t"); authorization != "" {
			want = append(want, trimmed)
		}
		if len(gotAuth) != len(want) || (len(want) == 1 && gotAuth[0] != want[0]) {
			t.Fatalf("upstream saw Authorization %q, client supplied %q", gotAuth, authorization)
		}
		if !strings.HasSuffix(gotPath, ".git/info/refs") || gotQuery != "service=git-upload-pack" {
			t.Fatalf("probe landed on %q?%q, not the ref advertisement for %q", gotPath, gotQuery, repoPath)
		}
		// Only the security direction is asserted: refusing a response this
		// pattern would have accepted costs a fetch, granting one it would have
		// rejected costs the repository.
		if result.GrantsRead() && !isAdvertisement(responseContentType, responseBody) {
			t.Fatalf("probe granted read for content type %q body %q", responseContentType, responseBody)
		}
	})
}

func isAdvertisement(contentType, body string) bool {
	mediaType, _, _ := strings.Cut(contentType, ";")
	return strings.EqualFold(strings.TrimSpace(mediaType), authztest.ContentType) && advertisementPattern.MatchString(body)
}

func isSendableHeaderValue(value string) bool {
	for _, c := range []byte(value) {
		if c < 0x20 || c == 0x7f {
			return false
		}
	}
	return true
}
