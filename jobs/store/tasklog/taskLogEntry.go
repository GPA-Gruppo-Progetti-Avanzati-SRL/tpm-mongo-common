package tasklog

import (
	"regexp"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// @tpm-schematics:start-region("top-file-section")
// @tpm-schematics:end-region("top-file-section")

type TaskLogEntry struct {
	Ts      string `json:"ts,omitempty" bson:"ts,omitempty" yaml:"ts,omitempty"`
	Level   string `json:"level,omitempty" bson:"level,omitempty" yaml:"level,omitempty"`
	Context string `json:"context,omitempty" bson:"context,omitempty" yaml:"context,omitempty"`
	Text    string `json:"text,omitempty" bson:"text,omitempty" yaml:"text,omitempty"`

	// @tpm-schematics:start-region("struct-section")

	zeroLogLevel zerolog.Level
	// @tpm-schematics:end-region("struct-section")
}

func (s TaskLogEntry) IsZero() bool {
	return s.Ts == "" && s.Level == "" && s.Text == ""
}

// @tpm-schematics:start-region("bottom-file-section")

var FormattedLevels = map[string]zerolog.Level{
	"TRC": zerolog.TraceLevel,
	"DBG": zerolog.DebugLevel,
	"INF": zerolog.InfoLevel,
	"WRN": zerolog.WarnLevel,
	"ERR": zerolog.ErrorLevel,
	"FTL": zerolog.FatalLevel,
	"PNC": zerolog.PanicLevel,
}

var logLineRegexp = regexp.MustCompile("^([0-9\\-T:+]*)\\s(TRC|DBG|INF|WRN|ERR|FTL|PNC)\\s([a-zA-Z:\\-0-9]*)\\s(.*)(?:\\r\\n|\\r|\\n)$")

func ParseLogLine(line string) (zerolog.Level, TaskLogEntry, error) {
	const semLogContext = "task-log-entry::parse-log-line"
	submatches := logLineRegexp.FindAllStringSubmatch(line, -1)

	if len(submatches) == 0 {
		log.Warn().Str("line", line).Msg(semLogContext + " - log pattern not recognized")
		return zerolog.NoLevel, TaskLogEntry{Text: line}, nil
	}

	if len(submatches[0]) != 5 {
		log.Warn().Int("len-matches", len(submatches[0])).Msg(semLogContext)
		return zerolog.NoLevel, TaskLogEntry{Text: line}, nil
	}

	e := TaskLogEntry{
		Ts:      submatches[0][1],
		Level:   submatches[0][2],
		Context: submatches[0][3],
		Text:    submatches[0][4],
	}

	return FormattedLevels[e.Level], e, nil
}

// @tpm-schematics:end-region("bottom-file-section")
