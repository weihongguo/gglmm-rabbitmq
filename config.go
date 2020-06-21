package rabbitmq

import "strings"

// ConfigRabbitmq --
type ConfigRabbitmq struct {
	User     string
	Password string
	Address  string
}

// Check --
func (config ConfigRabbitmq) Check() bool {
	if config.Address == "" || !strings.Contains(config.Address, ":") {
		return false
	}
	if config.User == "" || config.Password == "" {
		return false
	}
	return true
}
