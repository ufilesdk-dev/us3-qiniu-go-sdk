package main

import (
	"flag"
	"log"

	"github.com/ufilesdk-dev/us3-qiniu-go-sdk/syncdata/operation"
)

func main() {
	cf := flag.String("c", "cfg.toml", "config")
	f := flag.String("f", "file", "upload file")
	flag.Parse()

	x, err := operation.Load(*cf)
	if err != nil {
		log.Fatalln(err)
	}

	up := operation.NewUploader(x)

	err = up.Upload(*f, *f)
	log.Fatalln(err)
}
