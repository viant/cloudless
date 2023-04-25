package sam

type Config struct {
	Port     int
	Endpoint string
	Region   string
}

func (c *Config) Init() {
	if c.Endpoint == "" {
		c.Endpoint = "http://127.0.0.1:9001"
	}
	if c.Region == "" {
		c.Region = "us-west-2"
	}
	if c.Port == 0 {
		c.Port = 8081
	}
}
