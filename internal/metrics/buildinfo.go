package metrics

import (
	"runtime"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// RegisterBuildInfo publishes cachew.build_info, a gauge fixed at 1 whose
// labels identify the running build, so dashboards can show which commit each
// process runs.
func RegisterBuildInfo(version, gitCommit string) {
	NewConstGauge(otel.Meter("cachew"), "cachew.build_info", "",
		"Always 1; labels identify the running cachew build", 1,
		attribute.String("version", version),
		attribute.String("git_commit", gitCommit),
		attribute.String("go_version", runtime.Version()))
}
