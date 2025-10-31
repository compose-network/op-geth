package xlog

import (
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
)

// Logger wraps zerolog.Logger with additional context.
type Logger struct {
	zerolog.Logger
}

// New creates a new logger instanceproto.
func New(level string, pretty bool) Logger {
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	zerolog.TimeFieldFormat = time.RFC3339Nano

	logLevel := zerolog.InfoLevel
	switch strings.ToLower(level) {
	case "trace":
		logLevel = zerolog.TraceLevel
	case "debug":
		logLevel = zerolog.DebugLevel
	case "info":
		logLevel = zerolog.InfoLevel
	case "warn":
		logLevel = zerolog.WarnLevel
	case "error":
		logLevel = zerolog.ErrorLevel
	case "fatal":
		logLevel = zerolog.FatalLevel
	case "panic":
		logLevel = zerolog.PanicLevel
	}

	zerolog.SetGlobalLevel(logLevel)

	var zlog zerolog.Logger
	if pretty {
		output := zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: "15:04:05.000",
		}
		zlog = zerolog.New(output)
	} else {
		zlog = zerolog.New(os.Stdout)
	}

	zlog = zlog.With().
		Timestamp().
		Caller().
		Stack().
		Logger()

	return Logger{zlog}
}

// With creates a child logger with additional context.
func (l Logger) With() zerolog.Context {
	return l.Logger.With()
}

// Module creates a logger for a specific module.
func (l Logger) Module(name string) Logger {
	return Logger{l.With().Str("module", name).Logger()}
}
