package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("mongo2mysql <config.json>")
		os.Exit(1)
	}
	cfg, err := loadConfig(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	for _, table := range cfg.Tables {
		if err := table.Process(cfg); err != nil {
			log.Fatal(err)
		}
	}
}

func loadConfig(file string) (*Config, error) {
	f, err := os.Open(file)
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
