package xmetrics

// Common buckets for different types of measurements
var (
	// DurationBuckets for request/operation durations (1ms to 30s)
	DurationBuckets = []float64{
		.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30,
	}

	// SizeBuckets for message/payload sizes (1KB to 100MB)
	SizeBuckets = []float64{
		1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 268435456,
	}

	// CountBuckets for batch sizes, participant counts, etc.
	CountBuckets = []float64{
		1, 2, 5, 10, 25, 50, 100, 250, 500, 1000,
	}

	// NetworkBuckets for network connection durations (1s to 1hr)
	NetworkBuckets = []float64{
		1, 5, 15, 30, 60, 300, 600, 1800, 3600,
	}

	// ConsensusBuckets for consensus operations (100ms to 3min)
	ConsensusBuckets = []float64{
		0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 180,
	}
)
