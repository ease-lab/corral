package corral

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func loadConfig() {
	viper.SetConfigName("corralrc")
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.corral")

	setupDefaults()

	if err := viper.ReadInConfig(); err != nil {
		log.Fatal("could not load config: ", err)
	}

	viper.SetEnvPrefix("corral")
	viper.AutomaticEnv()
}

func setupDefaults() {
	defaultSettings := map[string]interface{}{
		"lambdaFunctionName": "corral_function",
		"lambdaMemory":       1500,
		"lambdaTimeout":      180,
		"lambdaManageRole":   true,
		"cleanup":            true,
		"verbose":            false,
		"splitSize":          100 * 1024 * 1024, // Default input split size is 100Mb
		"mapBinSize":         512 * 1024 * 1024, // Default map bin size is 512Mb
		"reduceBinSize":      512 * 1024 * 1024, // Default reduce bin size is 512Mb
		"maxConcurrency":     500,               // Maximum number of concurrent executors
		"workingLocation":    ".",
	}
	for key, value := range defaultSettings {
		viper.SetDefault(key, value)
	}

	aliases := map[string]string{
		"verbose":          "v",
		"working_location": "o",
	}
	for key, alias := range aliases {
		viper.RegisterAlias(alias, key)
	}
}
