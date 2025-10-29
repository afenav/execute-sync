package config

import (
	"flag"
	"testing"

	"github.com/urfave/cli/v2"
)

func newTestContext(t *testing.T, args []string) *cli.Context {
	t.Helper()
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	for _, f := range GetFlags() {
		if err := f.Apply(flagSet); err != nil {
			t.Fatalf("applying flag: %v", err)
		}
	}
	if err := flagSet.Parse(args); err != nil {
		t.Fatalf("parsing flags: %v", err)
	}

	app := &cli.App{Flags: GetFlags()}
	return cli.NewContext(app, flagSet, nil)
}

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("EXECUTESYNC_EXECUTE_URL", "https://example.com")
	t.Setenv("EXECUTESYNC_EXECUTE_APIKEY_ID", "id")
	t.Setenv("EXECUTESYNC_EXECUTE_APIKEY_SECRET", "secret")
	t.Setenv("EXECUTESYNC_DATABASE_TYPE", "POSTGRES")
	t.Setenv("EXECUTESYNC_DATABASE_DSN", "postgres://user:pass@host/db")
}

func TestResolveConfigAppliesDefaults(t *testing.T) {
	setRequiredEnv(t)
	ctx := newTestContext(t, nil)

	cfg := ResolveConfig(ctx)

	if cfg.MaxDocuments != 10000 {
		t.Fatalf("expected default max documents 10000, got %d", cfg.MaxDocuments)
	}
	if cfg.Wait != 600 {
		t.Fatalf("expected default wait 600, got %d", cfg.Wait)
	}
	if cfg.StateDir != "." {
		t.Fatalf("expected default state dir '.', got %q", cfg.StateDir)
	}
	if cfg.IncludeCalcs {
		t.Fatal("expected default include calcs false")
	}
}

func TestResolveConfigEnvironmentOverridesDefaults(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("EXECUTESYNC_WAIT", "42")
	ctx := newTestContext(t, nil)

	cfg := ResolveConfig(ctx)

	if cfg.Wait != 42 {
		t.Fatalf("expected wait overridden by environment to 42, got %d", cfg.Wait)
	}
}

func TestResolveConfigCLIOverridesEnvironment(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("EXECUTESYNC_WAIT", "42")
	ctx := newTestContext(t, []string{"--wait", "7"})

	cfg := ResolveConfig(ctx)

	if cfg.Wait != 7 {
		t.Fatalf("expected wait overridden by CLI to 7, got %d", cfg.Wait)
	}
}
