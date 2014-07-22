package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	//"runtime"

	"github.com/madelfio/wiki-place-rank/geowiki"

	//"github.com/davecheney/profile"
)

func main() {
	//defer profile.Start(profile.CPUProfile).Stop()

	if len(os.Args) < 2 {
		fmt.Println("Not enough arguments.\n")
		usage()
		return
	}

	//runtime.GOMAXPROCS(runtime.NumCPU()) // Mostly I/O bound, but why not

	switch cmd := os.Args[1]; cmd {

	case "locations":
		if len(os.Args) < 4 {
			fmt.Println("Not enough arguments.\n")
			fmt.Println("Usage: wpr locations <source_file> <dest_file>")
			return
		}

		infile := os.Args[2]
		outfile := os.Args[3]

		log.Println("Extracting location refs from geonames")

		err := geowiki.Locations(infile, outfile)
		if err != nil {
			log.Panic(err)
		}

	case "graph":
		if len(os.Args) < 4 {
			fmt.Println("Not enough arguments.\n")
			fmt.Println("Usage: wpr graph <source_file> <dest_file>")
			return
		}

		infile := os.Args[2]
		outfile := os.Args[3]

		log.Println("Extracting wikipedia graph")

		err := geowiki.Graph(infile, outfile)
		if err != nil {
			log.Panic(err)
		}

	case "pagerank":
		if len(os.Args) < 4 {
			fmt.Println("Not enough arguments.\n")
			fmt.Println("Usage: wpr pagerank <source_file> <dest_file>")
			return
		}

		infile := os.Args[2]
		outfile := os.Args[3]

		log.Println("Computing PageRank for all wikipedia pages")

		err := geowiki.RankPages(infile, outfile)
		if err != nil {
			log.Panic(err)
		}

	case "georank":
		if len(os.Args) < 5 {
			fmt.Println("Not enough arguments.\n")
			fmt.Println("Usage: wpr georank <source_file> <geo_file> <dest_file>")
			return
		}

		infile := os.Args[2]
		geofile := os.Args[3]
		outfile := os.Args[4]

		log.Println("Computing GeoRanks for location pages")

		err := geowiki.RankGeo(infile, geofile, outfile)
		if err != nil {
			log.Panic(err)
		}

	case "all":
		if len(os.Args) < 5 {
			fmt.Println("Not enough arguments.\n")
			fmt.Println("Usage: wpr all <wiki_dump> <geonames_dump> <dest_file>")
			return
		}

		infile := os.Args[2]
		geofile := os.Args[3]
		outfile := os.Args[4]

		tempdir, err := ioutil.TempDir("", "wiki-page-rank")
		if err != nil {
			log.Panic(err)
		}

		log.Printf("Temporary files will be stored in '%s'", tempdir)

		temp1 := tempdir + "/1-graph.gob"
		temp2 := tempdir + "/2-page-rank.gob"
		temp3 := tempdir + "/3-locations.gob"

		log.Println("Extracting wikipedia graph")

		err = geowiki.Graph(infile, temp1)
		if err != nil {
			log.Panic(err)
		}

		log.Println("Computing PageRank for all wikipedia pages")

		err = geowiki.RankPages(temp1, temp2)
		if err != nil {
			log.Panic(err)
		}

		log.Println("Extracting location refs from geonames")

		err = geowiki.Locations(geofile, temp3)
		if err != nil {
			log.Panic(err)
		}

		log.Println("Computing GeoRanks for location pages")

		err = geowiki.RankGeo(temp2, temp3, outfile)
		if err != nil {
			log.Panic(err)
		}

	default:
		fmt.Println("Unknown command:", cmd)
		usage()
	}
}

func usage() {
	fmt.Println("Usage: wpr <command> [options]")
	fmt.Println()
	fmt.Println("  Available commands:")
	fmt.Println("    locations - extract location refs from geonames")
	fmt.Println("    graph     - extract wikipedia graph")
	fmt.Println("    pagerank  - compute pagerank for location pages")
}
