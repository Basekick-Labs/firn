// Subprocess entrypoint for compaction jobs.
// Reads a JSON-encoded SubprocessConfig from stdin, runs the compaction job,
// and writes a JSON-encoded SubprocessResult to stdout.
// Must not import anything from cmd/firn.
package main

import (
	"encoding/json"
	"os"

	"github.com/basekick-labs/firn/internal/compaction"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	var cfg compaction.SubprocessConfig
	if err := json.NewDecoder(os.Stdin).Decode(&cfg); err != nil {
		writeError(err)
		os.Exit(1)
	}

	result := compaction.RunSubprocess(cfg)

	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		os.Exit(1)
	}

	if !result.Success {
		os.Exit(1)
	}
}

func writeError(err error) {
	_ = json.NewEncoder(os.Stdout).Encode(compaction.SubprocessResult{
		Success: false,
		Error:   err.Error(),
	})
}
