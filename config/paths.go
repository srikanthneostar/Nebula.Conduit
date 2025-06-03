package config

type PathConfig struct {
	PythonScriptsHome string
	AllowedPaths      []string
}

func (c *Config) GetPathConfig() *PathConfig {
	return &PathConfig{
		PythonScriptsHome: c.Paths.PythonScriptsHome,
		AllowedPaths:      c.Paths.AllowedPaths,
	}
}
