package config

import "github.com/metacubex/mihomo/component/resource"

func preprocessEnv(buf []byte) []byte {
	return resource.PreprocessEnv(buf)
}
