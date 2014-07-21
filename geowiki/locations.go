package geowiki

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
)

type GeoEntry struct {
	Id   int
	Name string
	Wiki string
}

var name_re = regexp.MustCompile(`<gn:name>([^<]+)<`)
var wiki_re = regexp.MustCompile(`<gn:wikipediaArticle rdf:resource="http://en.wikipedia.org/wiki/([^"]+)"`)
var id_re = regexp.MustCompile(`rdf:about="http://sws.geonames.org/(\d+)/"`)

func yieldLocationEntries(infile string, cp chan *GeoEntry) {
	defer close(cp)

	f, err := os.Open(infile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	bf := bufio.NewReaderSize(f, 25000)

	var wiki, name, id_str []string

	for {
		line, isPrefix, err := bf.ReadLine()

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatal(err)
		}

		if isPrefix {
			log.Println("Long Line")
			log.Fatal("Error: Unexpected long line reading", f.Name())
		}

		l := string(line)

		if wiki = wiki_re.FindStringSubmatch(l); wiki != nil {
			name = name_re.FindStringSubmatch(l)
			id_str = id_re.FindStringSubmatch(l)

			if name == nil {
				log.Panic("No name for entry:", wiki, name, id_str)
			}

			if id_str == nil {
				log.Panic("No id for entry:", wiki, name, id_str)
			}

			id, err := strconv.Atoi(id_str[1])
			if err != nil {
				log.Panic(err)
			}

			cp <- &GeoEntry{
				Id:   id,
				Name: name[1],
				Wiki: wiki[1],
			}
		}
	}
}

func WriteLocations(outfile string, numLocations int, cp chan *GeoEntry, done chan bool) {
	of, err := os.Create(outfile)
	if err != nil {
		log.Panic(err)
	}
	defer of.Close()

	gobEncoder := gob.NewEncoder(of)
	gobEncoder.Encode(numLocations)

	for loc := range cp {
		if loc != nil {
			err = gobEncoder.Encode(loc)
			if err != nil {
				panic(err)
			}
		} else {
			log.Printf("Null loc while writing locs")
		}
	}
	done <- true
}

func Locations(infile, outfile string) error {
	locations := make([]*GeoEntry, 0, 1000000)

	locInputChan := make(chan *GeoEntry, 20000)
	go yieldLocationEntries(infile, locInputChan)
	num := 0
	for loc := range locInputChan {
		num++
		locations = append(locations, loc)
	}
	log.Println("Found:", num, "locations with Wikipedia links")
	log.Println("Writing to:", outfile)

	locOutputChan := make(chan *GeoEntry, 20000)
	done := make(chan bool)
	go WriteLocations(outfile, num, locOutputChan, done)
	for _, loc := range locations {
		locOutputChan <- loc
	}
	close(locOutputChan)
	<-done

	log.Println("Done writing")

	return nil
}
