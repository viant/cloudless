package processor

import (
	"github.com/viant/assertly"
	"github.com/viant/tapper/config"
	"github.com/viant/toolbox"
	"testing"
	"time"
)

func TestConfig_ExpandDestination(t *testing.T) {
	var useCases = []struct {
		description string
		conf  *Config
		expectedResponse string
	}{
		{
			description : "test stream without rotation",
			conf: &Config{Concurrency: 5,
				DestinationURL: "mem://localhost/dest/sum-$UUID.txt",
				DestinationCodec: "gzip",
				MaxExecTimeMs:  2000,
			},
			expectedResponse: `{
								"Codec": "gzip",
								"StreamUpload": false,
								"URL": "~/mem://localhost/dest/sum-[a-z0-9.-]+/"	
								}`,
		},
		{
			description : "test stream with rotation and destination codec only",
			conf: &Config{Concurrency: 5,
				MaxExecTimeMs:  2000,
				DestinationCodec: "gzip",
				Destination: &config.Stream{
					URL:          "mem://localhost/local/sum-$UUID.txt",
					Rotation:     &config.Rotation{
						EveryMs:    100000000,
						URL:        "mem://localhost/dest/sum-$UUID.txt",
						Codec:      "gzip",
					},
				},
			},
			expectedResponse: `{
							"Codec": "gzip",
							"Rotation": {
								"Codec": "gzip",
								"EveryMs": 100000000,
								"URL": "~/mem://localhost/dest/sum-[a-z0-9.-]+/"
							},
							"StreamUpload": false,
							"URL": "~/mem://localhost/local/sum-[a-z0-9.-]+/"}`,
		},
		{
			description : "test stream with mixed attributes",
			conf: &Config{Concurrency: 5,
				MaxExecTimeMs:  2000,
				DestinationCodec: "gzip",
				DestinationURL: "mem://localhost/dest/sum-$UUID.txt",
				Destination: &config.Stream{
					Rotation:     &config.Rotation{
						EveryMs:    100000000,
						URL:        "mem://localhost/dest/sum-$UUID.txt",
						Codec:      "gzip",
					},
				},
			},
			expectedResponse: `{
								"Codec": "gzip",
								"Rotation": {
									"Codec": "gzip",
									"EveryMs": 100000000,
									"URL": "~/mem://localhost/dest/sum-[a-z0-9.-]+/"
								},
								"StreamUpload": false,
								"URL": "~/mem://localhost/dest/sum-[a-z0-9.-]+/"}`,
		},
		{
			description : "test stream with rotation only",
			conf: &Config{Concurrency: 5,
				MaxExecTimeMs:  2000,
				Destination: &config.Stream{
					Rotation:     &config.Rotation{
						EveryMs:    100000000,
						URL:        "mem://localhost/dest/sum-$UUID.txt",
						Codec:      "gzip",
					},
				},
			},
			expectedResponse: `{
								"Rotation": {
									"Codec": "gzip",
									"EveryMs": 100000000,
									"URL": "~/mem://localhost/dest/sum-[a-z0-9.-]+/"
								},
								"StreamUpload": false,
								"URL": "~/mem://localhost/dest/sum-[a-z0-9.-]+/"
							}`,
		},
	}
	for _, useCase := range useCases {
		startTime := time.Now()
		config := useCase.conf
		destination := config.ExpandDestination(startTime)
		if !assertly.AssertValues(t, useCase.expectedResponse, destination, useCase.description) {
			toolbox.DumpIndent(destination, true)
		}
	}
}
