# Artifactory caching proxy strategy
# strategy artifactory "example.jfrog.io" {
#   target = "https://example.jfrog.io"
# }

state = "./state"
url = "http://127.0.0.1:8080"
log {
  level = "debug"
}

opa {
  policy = <<EOF
  package cachew.authz
  default allow := false
  allow if startswith(input.remote_addr, "127.0.0.1:")
  allow if not input.path[0] in {"api", "admin", "metrics"}
  EOF
  test = <<EOF
  package cachew.authz_test
  import data.cachew.authz

  test_localhost_allowed if authz.allow with input as {"remote_addr": "127.0.0.1:1234", "path": ["api"]}
  test_remote_strategy_allowed if authz.allow with input as {"remote_addr": "10.0.0.1:1234", "path": ["git"]}
  test_remote_api_denied if not authz.allow with input as {"remote_addr": "10.0.0.1:1234", "path": ["api"]}
  test_remote_admin_denied if not authz.allow with input as {"remote_addr": "10.0.0.1:1234", "path": ["admin"]}
  test_remote_metrics_denied if not authz.allow with input as {"remote_addr": "10.0.0.1:1234", "path": ["metrics"]}
  EOF
}

# github-app {
#   app-id = "app-id-value"
#   private-key-path = "private-key-path-value"
#   installations = { "myorg" : "installation-id" }
# }

metrics {}

strategy git {
  #bundle-interval = "24h"
  snapshot-interval = "1h"
  repack-interval = "1h"
  # Serve partial-clone snapshots for repos whose history dwarfs their checkout.
  #snapshot-filters = {
  #  "github.com/org/repo": "blob:none",
  #}
  # Unsafe. Skips the per-request check that the client itself can read the
  # repository upstream, for the git strategy's own /git/ routes only.
  #disable-upstream-authz = true
}

# The object API is always enabled; this block only configures it.
strategy apiv1 {
  # Unsafe, and independent of the git strategy's disable-upstream-authz.
  # Skips the same check for reads of git objects through
  # GET/HEAD /api/v1/object/git/<key>.
  #disable-git-object-authz = true
}

strategy host "https://ghcr.io" {
  headers = {
    "Authorization": "Bearer QQ=="
  }
}

strategy host "https://w3.org" {}

strategy github-releases {
  token = "${GITHUB_TOKEN}"
  private-orgs = ["alecthomas"]
}

strategy gomod {
  proxy = "https://proxy.golang.org"
}

strategy hermit { }

strategy proxy { }

cache disk {
  limit-mb = 250000
  max-ttl = "8h"
}

metadata memory {}
