package utils

import (
	"encoding/json"
	"go_consumer/src"
	"log"
	"os"
)

func GetConfig(configFile string) (*src.Config, error) {
	file, err := os.Open(configFile)

	defer func() {
		if file != nil {
			err := file.Close()

			if err != nil {
				log.Println(err)
			}
		}
	}()

	if err != nil {
		return nil, err
	}

	dec := json.NewDecoder(file)
	conf := src.Config{}

	err = dec.Decode(&conf)
	if err != nil {
		return nil, err
	}

	return &conf, nil
}

func CheckFatal(err error){
	if err != nil {
		log.Fatal(err)
	}
}
