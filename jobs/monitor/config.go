package monitor

type Config struct {
	JobTypes []string `yaml:"job-types,omitempty" mapstructure:"job-types,omitempty" json:"job-types,omitempty"`
}
