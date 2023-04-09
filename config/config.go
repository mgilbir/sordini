package config

import (
	"os"
	"time"
)

// Config holds the configuration for a Config.
type Config struct {
	ID                            int32
	NodeName                      string
	DataDir                       string
	DevMode                       bool
	Addr                          string
	Bootstrap                     bool
	BootstrapExpect               int
	StartAsLeader                 bool
	StartJoinAddrsLAN             []string
	StartJoinAddrsWAN             []string
	NonVoter                      bool
	RaftAddr                      string
	LeaveDrainTime                time.Duration
	ReconcileInterval             time.Duration
	OffsetsTopicReplicationFactor int16
}

// DefaultConfig creates/returns a default configuration.
func DefaultConfig() *Config {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	conf := &Config{
		DevMode:                       false,
		NodeName:                      hostname,
		LeaveDrainTime:                5 * time.Second,
		ReconcileInterval:             60 * time.Second,
		OffsetsTopicReplicationFactor: 3,
	}

	return conf
}
