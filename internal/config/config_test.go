package config //nolint:testpackage

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/hcl/v2"

	"github.com/tenstorrent/internal-cachew/internal/cache"
	"github.com/tenstorrent/internal-cachew/internal/logging"
	"github.com/tenstorrent/internal-cachew/internal/metadatadb"
	"github.com/tenstorrent/internal-cachew/internal/strategy"
)

func TestUnwrapBlock(t *testing.T) {
	tests := []struct {
		name           string
		block          *hcl.Block
		expectedName   string
		expectedLabels []string
		expectedErr    string
	}{
		{
			name:           "SimpleBlock",
			block:          &hcl.Block{Name: "cache", Labels: []string{"disk"}},
			expectedName:   "disk",
			expectedLabels: []string{},
		},
		{
			name:           "BlockWithExtraLabels",
			block:          &hcl.Block{Name: "strategy", Labels: []string{"host", "https://ghcr.io"}},
			expectedName:   "host",
			expectedLabels: []string{"https://ghcr.io"},
		},
		{
			name:        "MissingLabel",
			block:       &hcl.Block{Name: "cache"},
			expectedErr: "cache block requires a name label",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, inner, err := unwrapBlock(tt.block)
			if tt.expectedErr != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedName, name)
			assert.Equal(t, tt.expectedName, inner.Name)
			assert.Equal(t, tt.expectedLabels, inner.Labels)
		})
	}
}

func TestInjectEnvars(t *testing.T) {
	type Scheduler struct {
		Concurrency int `hcl:"concurrency"`
	}
	type GitClone struct {
		Depth int    `hcl:"depth"`
		Dir   string `hcl:"dir"`
	}
	type Config struct {
		Bind      string    `hcl:"bind"`
		Scheduler Scheduler `hcl:"scheduler,block"`
		GitClone  GitClone  `hcl:"git-clone,block"`
	}

	schema, err := hcl.Schema(new(Config))
	assert.NoError(t, err)

	tests := []struct {
		name     string
		config   string
		vars     map[string]string
		expected string
	}{
		{
			name:   "InjectTopLevelAttr",
			config: ``,
			vars:   map[string]string{"CACHEW_BIND": "0.0.0.0:9090"},
			expected: `
bind = "0.0.0.0:9090"
`,
		},
		{
			name:   "InjectNestedAttr",
			config: `bind = "127.0.0.1:8080"`,
			vars:   map[string]string{"CACHEW_SCHEDULER_CONCURRENCY": "10"},
			expected: `
bind = "127.0.0.1:8080"

scheduler {
  concurrency = 10
}
`,
		},
		{
			name: "ExistingAttrNotOverwritten",
			config: `
bind = "127.0.0.1:8080"

scheduler {
  concurrency = 4
}
`,
			vars: map[string]string{"CACHEW_SCHEDULER_CONCURRENCY": "10"},
			expected: `
bind = "127.0.0.1:8080"

scheduler {
  concurrency = 4
}
`,
		},
		{
			name: "InjectIntoExistingBlock",
			config: `
git-clone {
  depth = 1
}
`,
			vars: map[string]string{"CACHEW_GIT_CLONE_DIR": "/tmp/clones"},
			expected: `
git-clone {
  depth = 1
  dir = "/tmp/clones"
}
`,
		},
		{
			name:   "NoMatchingEnvar",
			config: `bind = "127.0.0.1:8080"`,
			vars:   map[string]string{"UNRELATED_VAR": "foo"},
			expected: `
bind = "127.0.0.1:8080"
`,
		},
		{
			name:     "EmptyBlockNotCreated",
			config:   ``,
			vars:     map[string]string{},
			expected: ``,
		},
		{
			name:   "MultipleInjections",
			config: ``,
			vars: map[string]string{
				"CACHEW_BIND":                  "0.0.0.0:9090",
				"CACHEW_SCHEDULER_CONCURRENCY": "8",
				"CACHEW_GIT_CLONE_DEPTH":       "3",
			},
			expected: `
bind = "0.0.0.0:9090"

scheduler {
  concurrency = 8
}

git-clone {
  depth = 3
}
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := hcl.Parse(strings.NewReader(tt.config))
			assert.NoError(t, err)

			InjectEnvars(schema, config, "CACHEW", tt.vars)

			got, err := hcl.MarshalAST(config)
			assert.NoError(t, err)
			assert.Equal(t, strings.TrimSpace(tt.expected), strings.TrimSpace(string(got)))
		})
	}
}

func TestInjectEnvarsExpandsPlaceholders(t *testing.T) {
	type OPA struct {
		Policy string `hcl:"policy"`
	}
	type Config struct {
		Bind      string `hcl:"bind"`
		OPAConfig OPA    `hcl:"opa,block"`
	}
	schema, err := hcl.Schema(new(Config))
	assert.NoError(t, err)

	const input = `
bind = "${CACHEW_BIND}"

opa {
  policy = <<EOF
  allow if caller_principal == "${CACHEW_WARMER_PRINCIPAL}"
  EOF
}
`
	ast, err := hcl.Parse(strings.NewReader(input))
	assert.NoError(t, err)

	InjectEnvars(schema, ast, "CACHEW", map[string]string{
		"CACHEW_BIND":             "0.0.0.0:9090",
		"CACHEW_WARMER_PRINCIPAL": "spiffe://example/ns/warm/sa/x",
	})

	got, err := hcl.MarshalAST(ast)
	assert.NoError(t, err)
	out := string(got)

	// Both *hcl.String and *hcl.Heredoc attribute values are expanded.
	assert.Contains(t, out, `bind = "0.0.0.0:9090"`)
	assert.Contains(t, out, `caller_principal == "spiffe://example/ns/warm/sa/x"`)
	// No literal placeholder remains anywhere in the rendered AST.
	assert.Equal(t, false, strings.Contains(out, "${CACHEW_"))
}

func TestLoadRequiresMetadataBackend(t *testing.T) {
	cr := cache.NewRegistry()
	cache.RegisterMemory(cr)
	mr := metadatadb.NewRegistry()
	metadatadb.RegisterMemory(mr)
	sr := strategy.NewRegistry()

	ast, err := hcl.Parse(strings.NewReader(`cache memory {}`))
	assert.NoError(t, err)

	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	_, _, err = Load(ctx, cr, mr, sr, ast, http.NewServeMux(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected a metadata backend")
}

// TestLoadAPIV1Block pins that the object API can be configured explicitly. The
// strategy is enabled implicitly, so an explicit block must replace that
// implicit one rather than adding a second: building apiv1 twice registers its
// routes twice, which http.ServeMux panics on.
func TestLoadAPIV1Block(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		wantErr bool
	}{
		{name: "ImplicitWhenAbsent", config: ""},
		{name: "ExplicitEmpty", config: `strategy apiv1 {}`},
		{name: "ExplicitFlagAccepted", config: `strategy apiv1 { disable-git-object-authz = true }`},
		{name: "ExplicitFlagAlongsideOtherStrategies",
			config: "strategy hermit {}\nstrategy apiv1 { disable-git-object-authz = true }"},
		{name: "DuplicateExplicitRejected", config: "strategy apiv1 {}\nstrategy apiv1 {}", wantErr: true},
		{name: "DuplicateExplicitWithFlagRejected",
			config: "strategy apiv1 {}\nstrategy apiv1 { disable-git-object-authz = true }", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := "cache memory {}\nmetadata memory {}\n" + test.config + "\n"
			ast, err := hcl.Parse(strings.NewReader(source))
			assert.NoError(t, err)

			classified, err := classifyBlocks(ast)
			if test.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			apiV1Blocks := 0
			for _, block := range classified.strategies {
				if block.Name == apiV1Strategy {
					apiV1Blocks++
				}
			}
			assert.Equal(t, 1, apiV1Blocks)

			cr := cache.NewRegistry()
			cache.RegisterMemory(cr)
			mr := metadatadb.NewRegistry()
			metadatadb.RegisterMemory(mr)
			sr := strategy.NewRegistry()
			strategy.RegisterAPIV1(sr, nil)
			strategy.RegisterHermit(sr)

			ctx := logging.ContextWithLogger(context.Background(), slog.Default())
			_, _, err = Load(ctx, cr, mr, sr, ast, http.NewServeMux(), nil)
			assert.NoError(t, err)
		})
	}
}
