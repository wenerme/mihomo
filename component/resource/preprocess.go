package resource

import (
	"os"
	"regexp"

	"github.com/metacubex/mihomo/log"
)

var envPattern = regexp.MustCompile(`\{\{env\.([A-Za-z_][A-Za-z0-9_]*)\}\}`)

// PreprocessEnv replaces {{env.VARNAME}} patterns with environment variable values.
// Undefined variables are replaced with empty strings and a warning is logged.
func PreprocessEnv(buf []byte) []byte {
	return envPattern.ReplaceAllFunc(buf, func(match []byte) []byte {
		sub := envPattern.FindSubmatch(match)
		name := string(sub[1])
		val, ok := os.LookupEnv(name)
		if !ok {
			log.Warnln("environment variable %s is not defined, replacing with empty string", name)
		}
		return []byte(val)
	})
}
