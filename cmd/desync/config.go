package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/folbricht/desync"
	"github.com/folbricht/desync/cmd/internal/desyncconfig"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// Type aliases so no other file in the package needs changes.
type Config = desyncconfig.Config
type S3Creds = desyncconfig.S3Creds

func newConfigCommand(ctx context.Context) *cobra.Command {
	var write bool

	cmd := &cobra.Command{
		Use:   "config",
		Short: "Show or write config file",
		Long: `Shows the current internal configuration settings, either the defaults,
the values from $HOME/.config/desync/config.json or the specified config file. The
output can be used to create a custom config file by writing it to the specified file
or $HOME/.config/desync/config.json by default.`,
		Example: `  desync config
  desync --config desync.json config -w`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runConfig(ctx, write)
		},
		SilenceUsage: true,
	}

	flags := cmd.Flags()
	flags.BoolVarP(&write, "write", "w", false, "write current configuration to file")
	return cmd
}

func runConfig(ctx context.Context, write bool) error {
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	var w io.Writer = os.Stdout
	if write {
		if err = os.MkdirAll(filepath.Dir(cfgFile), 0755); err != nil {
			return err
		}
		f, err := os.OpenFile(cfgFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			return err
		}
		defer f.Close()
		fmt.Println("Writing config to", cfgFile)
		w = f
	}
	_, err = w.Write(b)
	fmt.Println()
	return err
}

// Global config in the main package defining the defaults. Those can be
// overridden by loading a config file or in the command line.
var cfg Config
var cfgFile string

// Look for $HOME/.config/desync and if present, load into the global config
// instance. Values defined in the file will be set accordingly, while anything
// that's not in the file will retain its default values.
func initConfig() {
	var err error
	cfg, cfgFile, err = desyncconfig.LoadConfig(cfgFile)
	if err != nil {
		die(err)
	}
}

// Digest algorithm to be used by desync globally.
var digestAlgorithm string

func setDigestAlgorithm() {
	if err := desyncconfig.SetDigestAlgorithm(cfg.ResolveDigest(digestAlgorithm)); err != nil {
		die(err)
	}
}

// Verbose mode
var verbose bool

func setVerbose() {
	if verbose {
		desync.Log = &logrus.Logger{
			Out:       os.Stderr,
			Formatter: new(logrus.TextFormatter),
			// Hooks:     make(logrus.LevelHooks),
			Level: logrus.DebugLevel,
		}
	}
}
