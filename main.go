package main

import (
	"io/ioutil"
	"log"
	"os"

	"github.com/jessevdk/go-flags"
	"gopkg.in/yaml.v2"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
		return
	}
	parser := flags.NewParser(cfg, flags.HelpFlag|flags.PassDoubleDash)
	if _, err := parser.Parse(); err != nil {
		log.Fatal(err)
	}
	for _, table := range cfg.Tables {
		if err := table.Process(cfg); err != nil {
			log.Fatal(err)
		}
	}
}

func loadConfig() (*Config, error) {
	var fileConfig struct {
		ConfigFile string `long:"config" default:"config.yaml"`
	}
	parser := flags.NewParser(&fileConfig, flags.IgnoreUnknown)
	if _, err := parser.Parse(); err != nil {
		return nil, err
	}
	f, err := os.Open(fileConfig.ConfigFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	cfg := new(Config)
	return cfg, yaml.Unmarshal(buf, cfg)
}
