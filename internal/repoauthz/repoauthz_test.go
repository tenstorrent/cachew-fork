package repoauthz_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/tenstorrent/internal-cachew/internal/repoauthz"
	"github.com/tenstorrent/internal-cachew/internal/repoauthz/authztest"
)

const privatePrefix = "/org/private"

// newUpstream serves anonymous ref advertisements for every repository except
// those under privatePrefix, which need credentials — the shape of decision a
// real host presents.
func newUpstream(t *testing.T, credentials string, probes *atomic.Int64) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes.Add(1)
		if strings.HasPrefix(r.URL.Path, privatePrefix) && r.Header.Get("Authorization") != credentials {
			w.Header().Set("WWW-Authenticate", `Basic realm="upstream"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		authztest.WriteAdvertisement(w)
	}))
	t.Cleanup(server.Close)
	return server
}

func TestAuthorize(t *testing.T) {
	const credentials = "Bearer client-token"
	var probes atomic.Int64
	server := newUpstream(t, credentials, &probes)

	tests := []struct {
		name          string
		repoPath      string
		authorization string
		want          repoauthz.Outcome
		wantAllowed   bool
	}{
		{name: "PublicRepoNeedsNoCredentials", repoPath: "/org/public", want: repoauthz.OutcomePublic, wantAllowed: true},
		{name: "PublicRepoIgnoresCredentials", repoPath: "/org/public", authorization: "Bearer wrong",
			want: repoauthz.OutcomePublic, wantAllowed: true},
		{name: "PrivateRepoWithoutCredentials", repoPath: privatePrefix, want: repoauthz.OutcomeNoCredentials},
		{name: "PrivateRepoWithWrongCredentials", repoPath: privatePrefix, authorization: "Bearer wrong",
			want: repoauthz.OutcomeRejected},
		{name: "PrivateRepoWithGoodCredentials", repoPath: privatePrefix, authorization: credentials,
			want: repoauthz.OutcomeAllowed, wantAllowed: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a := repoauthz.NewAuthorizer(server.Client(), time.Hour)
			outcome, err := a.Authorize(t.Context(), server.URL+test.repoPath, test.authorization)
			assert.NoError(t, err)
			assert.Equal(t, test.want, outcome)
			assert.Equal(t, test.wantAllowed, outcome.Allowed())
		})
	}
}

// TestProbeRefsRequiresAnAdvertisement is the security half of the probe: a
// status is not a grant. Upstreams that answer an unauthorized advertisement
// request with 200 and something that is not an advertisement — an SSO login
// page being the common one — must not be read as read access to a mirror
// cachew populated with its own credentials.
func TestProbeRefsRequiresAnAdvertisement(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		contentType string
		body        string
		want        bool
	}{
		{name: "Advertisement", contentType: authztest.ContentType, body: authztest.Advertisement, want: true},
		{name: "AdvertisementWithCharset", contentType: authztest.ContentType + "; charset=utf-8",
			body: authztest.Advertisement, want: true},
		{name: "AdvertisementWithUppercaseType", contentType: "Application/X-Git-Upload-Pack-Advertisement",
			body: authztest.Advertisement, want: true},
		{name: "LoginPage", contentType: "text/html; charset=utf-8", body: "<html>sign in</html>"},
		{name: "NoContentType", body: authztest.Advertisement},
		{name: "EmptyBody", contentType: authztest.ContentType},
		{name: "TruncatedServiceHeader", contentType: authztest.ContentType, body: "001e# service=git-up"},
		{name: "NonHexadecimalLength", contentType: authztest.ContentType, body: "zzzz# service=git-upload-pack\n0000"},
		{name: "ReceivePackService", contentType: authztest.ContentType, body: "0021# service=git-receive-pack\n0000"},
		{name: "AdvertisementBodyWithHTMLType", contentType: "text/html", body: authztest.Advertisement},
		{name: "Unauthorized", status: http.StatusUnauthorized, contentType: authztest.ContentType,
			body: authztest.Advertisement},
		{name: "NotFound", status: http.StatusNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if test.contentType != "" {
					w.Header().Set("Content-Type", test.contentType)
				}
				status := test.status
				if status == 0 {
					status = http.StatusOK
				}
				w.WriteHeader(status)
				_, _ = w.Write([]byte(test.body)) //nolint:errcheck // a test server has nowhere to report this
			}))
			t.Cleanup(server.Close)

			result, err := repoauthz.ProbeRefs(t.Context(), server.Client(), server.URL+"/org/repo", "Bearer token")
			assert.NoError(t, err)
			assert.Equal(t, test.want, result.GrantsRead())

			a := repoauthz.NewAuthorizer(server.Client(), time.Hour)
			outcome, err := a.Authorize(t.Context(), server.URL+"/org/repo", "Bearer token")
			assert.NoError(t, err)
			assert.Equal(t, test.want, outcome.Allowed(), "outcome %q disagrees with the probe", outcome)
		})
	}
}

// TestProbeRefsRedirectPolicy pins that a probe stays on the host whose
// repository is being authorized. net/http drops Authorization when a redirect
// changes host, so a followed cross-host redirect would answer a question about
// anonymous access with the authority of a client-credentialed one.
func TestProbeRefsRedirectPolicy(t *testing.T) {
	var sawAuth atomic.Value
	sawAuth.Store("")
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuth.Store(r.Header.Get("Authorization"))
		authztest.WriteAdvertisement(w)
	}))
	t.Cleanup(target.Close)

	tests := []struct {
		name     string
		redirect func(server *httptest.Server, r *http.Request) string
		want     bool
	}{
		{name: "NoRedirect", want: true},
		{name: "SameHostRedirect", want: true, redirect: func(server *httptest.Server, r *http.Request) string {
			if strings.HasPrefix(r.URL.Path, "/moved") {
				return ""
			}
			return server.URL + "/moved" + r.URL.Path + "?" + r.URL.RawQuery
		}},
		{name: "ForeignHostRedirect", redirect: func(_ *httptest.Server, r *http.Request) string {
			return target.URL + r.URL.Path + "?" + r.URL.RawQuery
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sawAuth.Store("")
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if test.redirect != nil {
					if to := test.redirect(server, r); to != "" {
						http.Redirect(w, r, to, http.StatusFound)
						return
					}
				}
				sawAuth.Store(r.Header.Get("Authorization"))
				authztest.WriteAdvertisement(w)
			}))
			t.Cleanup(server.Close)

			result, err := repoauthz.ProbeRefs(t.Context(), server.Client(), server.URL+"/org/repo", "Bearer token")
			assert.NoError(t, err)
			assert.Equal(t, test.want, result.GrantsRead())
			if !test.want {
				reached, _ := sawAuth.Load().(string)
				assert.Equal(t, "", reached, "the probe reached a host it was never authorized against")
			}
		})
	}
}

// TestProbeRefsStopsRedirectLoops pins the redirect cap that supplying a
// CheckRedirect takes over from net/http.
func TestProbeRefsStopsRedirectLoops(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, server.URL+r.URL.Path+"/deeper?"+r.URL.RawQuery, http.StatusFound)
	}))
	t.Cleanup(server.Close)

	_, err := repoauthz.ProbeRefs(t.Context(), server.Client(), server.URL+"/org/repo", "Bearer token")
	assert.Error(t, err)
}

func TestUnreachableUpstreamFailsClosed(t *testing.T) {
	server := newUpstream(t, "Bearer client-token", &atomic.Int64{})
	client := server.Client()
	url := server.URL + privatePrefix
	server.Close()

	a := repoauthz.NewAuthorizer(client, time.Hour)
	outcome, err := a.Authorize(t.Context(), url, "Bearer client-token")
	assert.Error(t, err)
	assert.Equal(t, repoauthz.OutcomeProbeError, outcome)
	assert.False(t, outcome.Allowed())
}

// TestVerdictCacheOverflowDoesNotWeakenDecisions is the security half of the
// bounded cache: evicting a verdict may cost an extra upstream probe, but a
// repository whose entry is gone must be re-probed and reach the same answer,
// never fall through as public.
func TestVerdictCacheOverflowDoesNotWeakenDecisions(t *testing.T) {
	const credentials = "Bearer client-token"
	var probes atomic.Int64
	server := newUpstream(t, credentials, &probes)
	a := repoauthz.NewAuthorizer(server.Client(), time.Hour)
	ctx := t.Context()
	private := server.URL + privatePrefix

	outcome, err := a.Authorize(ctx, private, "")
	assert.NoError(t, err)
	assert.Equal(t, repoauthz.OutcomeNoCredentials, outcome)

	cached := probes.Load()
	outcome, err = a.Authorize(ctx, private, "")
	assert.NoError(t, err)
	assert.Equal(t, repoauthz.OutcomeNoCredentials, outcome)
	assert.Equal(t, cached, probes.Load(), "a fresh verdict was re-probed")

	// Flood the cache with unrelated repositories until the private verdict has
	// certainly been evicted, then confirm the decision is unchanged.
	for i := range 12288 {
		requiresAuth, err := a.RequiresAuth(ctx, server.URL+"/org/flood"+strconv.Itoa(i))
		assert.NoError(t, err)
		assert.False(t, requiresAuth)
	}

	flooded := probes.Load()
	outcome, err = a.Authorize(ctx, private, "")
	assert.NoError(t, err)
	assert.Equal(t, repoauthz.OutcomeNoCredentials, outcome, "an evicted verdict was read as public")
	assert.True(t, probes.Load() > flooded, "the evicted verdict was answered without re-probing upstream")

	outcome, err = a.Authorize(ctx, private, credentials)
	assert.NoError(t, err)
	assert.Equal(t, repoauthz.OutcomeAllowed, outcome)
}
