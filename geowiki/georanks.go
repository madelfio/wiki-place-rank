//
// Apply filter based on geonames/wikipedia links to get normalized rank of
// geographic entities.
//
package geowiki

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
)

type RankedGeo struct {
	*GeoEntry
	*RankedPage
}

type RankedGeos []RankedGeo

func (r RankedGeos) Len() int { return len(r) }
func (r RankedGeos) Less(i, j int) bool {
	if r[i].RankedPage == nil {
		return false
	}
	if r[j].RankedPage == nil {
		return true
	}
	return r[i].Order < r[j].Order
}
func (r RankedGeos) Swap(i, j int) { r[i], r[j] = r[j], r[i] }

func ReadRankedPages(infile string, cp chan *RankedPage) {
	f, err := os.Open(infile)
	if err != nil {
		log.Panic(err)
	}

	defer f.Close()
	defer close(cp)

	gobDecoder := gob.NewDecoder(f)
	var numPages int
	gobDecoder.Decode(&numPages)

	for i := 0; i < numPages; i++ {
		var page RankedPage
		err := gobDecoder.Decode(&page)
		if err != nil {
			log.Panic(err)
		}
		cp <- &page
	}

}

func ReadGeoEntries(geofile string, cp chan *GeoEntry) {
	f, err := os.Open(geofile)
	if err != nil {
		log.Panic(err)
	}

	defer f.Close()
	defer close(cp)

	gobDecoder := gob.NewDecoder(f)
	var numLocations int
	gobDecoder.Decode(&numLocations)

	for i := 0; i < numLocations; i++ {
		var loc GeoEntry
		err := gobDecoder.Decode(&loc)
		if err != nil {
			log.Panic(err)
		}
		cp <- &loc
	}
}

func WriteGeoRanks(outfile string, numGeoRanks int, cp chan *RankedGeo, done chan bool) {
	// write json, gob, csv, or txt depending on extension
	of, err := os.Create(outfile)
	if err != nil {
		log.Panic(err)
	}
	defer of.Close()

	var writer func(*RankedGeo) error
	if strings.HasSuffix(outfile, ".txt") {
		log.Printf("Output in text format")
		writer = func(l *RankedGeo) error {
			fmt.Fprintf(of, "%d \"%s\" %s %.10f %d\n", l.GeoEntry.Id, l.GeoEntry.Name, l.GeoEntry.Wiki, l.Rank, l.Order)
			return nil
		}
	}

	for loc := range cp {
		if loc != nil {
			err = writer(loc)
			if err != nil {
				panic(err)
			}
		} else {
			log.Printf("Null loc while writing locs")
		}
	}
	done <- true
}

func RankGeo(infile, geofile, outfile string) (err error) {
	rankedGeoLookup := make(map[string]*RankedGeo, 10000)
	rankedLocations := make([]RankedGeo, 1000000)

	geoChan := make(chan *GeoEntry, 200)
	log.Printf("Reading Geo Entries from %s", geofile)
	go ReadGeoEntries(geofile, geoChan)

	i := 0
	for loc := range geoChan {
		rg := RankedGeo{
			GeoEntry:   loc,
			RankedPage: nil,
		}
		rankedLocations[i] = rg
		rankedGeoLookup[loc.Name] = &rankedLocations[i]
		i++
	}

	rpchan := make(chan *RankedPage, 30)
	log.Printf("Reading Ranked Pages from %s", infile)
	go ReadRankedPages(infile, rpchan)

	var found, missing uint

	for page := range rpchan {
		rankedGeo, ok := rankedGeoLookup[page.Title]
		if ok {
			rankedGeo.RankedPage = page
			if found < 10 {
				log.Printf("Found! page.Title: '%s'", rankedGeo.Title)
			}
			found++
		} else {
			if missing < 10 {
				log.Printf("Missing! page.Title: '%s'", page.Title)
			}
			missing++
		}
	}

	log.Printf("Found: %d, Missing: %d", found, missing)

	log.Printf("Sorting RankedGeos")
	rankedGeos := RankedGeos(rankedLocations)
	sort.Sort(rankedGeos)

	log.Printf("Writing ranked locations to '%s'", outfile)
	geoOutChan := make(chan *RankedGeo, 10000)
	done := make(chan bool)
	go WriteGeoRanks(outfile, len(rankedLocations), geoOutChan, done)
	for i := 0; i < len(rankedGeos); i++ {
		if rankedGeos[i].RankedPage == nil {
			continue
		}
		geoOutChan <- &rankedGeos[i]
	}
	close(geoOutChan)
	<-done

	log.Printf("Done writing")

	return
}
