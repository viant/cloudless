package sam

import (
	"context"
	"github.com/jessevdk/go-flags"
	"log"
)

func Run(args []string) {
	options := &Options{}
	_, err := flags.ParseArgs(options, args)
	if err != nil {
		log.Fatalln(err)
	}
	tmpl, err := NewTemplateWithURL(context.Background(), options.TemplateURL)
	if err != nil {
		log.Fatalln(err)
	}
	srv, err := New(tmpl, &Config{})
	if err != nil {
		log.Fatalln(err)
	}
	srv.Start()

}
