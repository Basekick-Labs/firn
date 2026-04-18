package main

import (
	"os"

	"github.com/basekick-labs/firn/internal/config"
	"github.com/basekick-labs/firn/internal/scheduler"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	root := &cobra.Command{
		Use:   "firn",
		Short: "Open source Iceberg table maintenance daemon",
		RunE:  run,
	}
	root.Flags().String("config", "firn.yaml", "Path to config file")

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, _ []string) error {
	cfgPath, _ := cmd.Flags().GetString("config")

	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}

	if cfg.Debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	s, err := scheduler.New(cfg)
	if err != nil {
		return err
	}

	return s.Run(cmd.Context())
}
