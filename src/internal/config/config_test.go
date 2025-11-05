package config

import (
	"flag"
	"io"
	"os"
	"testing"

	"github.com/urfave/cli/v2"
)

func TestResolveConfig_DefaultEnvFlagPriority(t *testing.T) {
	originalWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	tempDir := t.TempDir()
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("chdir temp dir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(originalWD)
	})

	t.Run("defaults when unset", func(t *testing.T) {
		setRequiredEnv(t)

		ctx := newCLIContext(t, nil)
		cfg := ResolveConfig(ctx)

		if cfg.MaxDocuments != 10000 {
			t.Fatalf("expected default MaxDocuments 10000, got %d", cfg.MaxDocuments)
		}
		if cfg.Wait != 600 {
			t.Fatalf("expected default Wait 600, got %d", cfg.Wait)
		}
	})

	t.Run("env overrides defaults including zero", func(t *testing.T) {
		setRequiredEnv(t)
		t.Setenv("EXECUTESYNC_MAX_DOCUMENTS", "123")
		t.Setenv("EXECUTESYNC_WAIT", "0")

		ctx := newCLIContext(t, nil)
		cfg := ResolveConfig(ctx)

		if cfg.MaxDocuments != 123 {
			t.Fatalf("expected env override MaxDocuments 123, got %d", cfg.MaxDocuments)
		}
		if cfg.Wait != 0 {
			t.Fatalf("expected env override Wait 0, got %d", cfg.Wait)
		}
	})

	t.Run("flags override env", func(t *testing.T) {
		setRequiredEnv(t)
		t.Setenv("EXECUTESYNC_MAX_DOCUMENTS", "555")
		t.Setenv("EXECUTESYNC_WAIT", "0")

		ctx := newCLIContext(t, []string{"--max-documents", "777", "--wait", "42"})
		cfg := ResolveConfig(ctx)

		if cfg.MaxDocuments != 777 {
			t.Fatalf("expected flag override MaxDocuments 777, got %d", cfg.MaxDocuments)
		}
		if cfg.Wait != 42 {
			t.Fatalf("expected flag override Wait 42, got %d", cfg.Wait)
		}
	})
}

func newCLIContext(t *testing.T, args []string) *cli.Context {
	t.Helper()

	app := cli.NewApp()
	app.Flags = GetFlags()

	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	flagSet.SetOutput(io.Discard)

	for _, f := range app.Flags {
		if err := f.Apply(flagSet); err != nil {
			t.Fatalf("apply flag %q: %v", f.Names()[0], err)
		}
	}

	if args == nil {
		args = []string{}
	}
	if err := flagSet.Parse(args); err != nil {
		t.Fatalf("parse args %v: %v", args, err)
	}

	return cli.NewContext(app, flagSet, nil)
}

func setRequiredEnv(t *testing.T) {
	t.Helper()

	t.Setenv("EXECUTESYNC_EXECUTE_URL", "https://example.com")
	t.Setenv("EXECUTESYNC_EXECUTE_APIKEY_ID", "id")
	t.Setenv("EXECUTESYNC_EXECUTE_APIKEY_SECRET", "secret")
	t.Setenv("EXECUTESYNC_DATABASE_TYPE", "SQLITE")
	t.Setenv("EXECUTESYNC_DATABASE_DSN", ":memory:")
}
