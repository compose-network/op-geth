package slot

import "time"

// Config holds slot timing configuration
type Config struct {
	Duration    time.Duration `mapstructure:"duration"     yaml:"duration"`
	SealCutover float64       `mapstructure:"seal_cutover" yaml:"seal_cutover"`
	GenesisTime time.Time     `mapstructure:"genesis_time" yaml:"genesis_time"`
}

func DefaultConfig() Config {
	return Config{
		Duration:    6 * time.Second, // Ethereum slot alignment
		SealCutover: 2.0 / 3.0,       // Seal at 8 seconds into 12-second slot
	}
}
