package geowiki

import (
	"encoding/gob"
	"log"
	"math"
	"os"
	"sort"
)

const (
	PageRankWalkProbability = 0.85
	PageRankConvergence     = 0.000001
)

type RankedPage struct {
	Page
	Order uint32
	Rank  float32
}

type RankedPageList []RankedPage

func (s RankedPageList) Len() int {
	return len(s)
}

func (s RankedPageList) Less(i, j int) bool {
	return s[i].Rank > s[j].Rank
}

func (s RankedPageList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func ReadNumPages(infile string) int {
	f, err := os.Open(infile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	gobDecoder := gob.NewDecoder(f)
	var numPages int
	gobDecoder.Decode(&numPages)

	return numPages
}

func ReadPages(infile string, cp chan *Page) {
	f, err := os.Open(infile)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	defer close(cp)

	gobDecoder := gob.NewDecoder(f)
	var numPages int
	gobDecoder.Decode(&numPages)

	for i := 0; i < numPages; i++ {
		var page Page
		err := gobDecoder.Decode(&page)
		if err != nil {
			log.Panic(err)
		}
		cp <- &page
	}
}

func pageRank(pages []Page) []float64 {
	beta, epsilon := PageRankWalkProbability, PageRankConvergence
	log.Printf("Ranking with beta='%f', epsilon='%f'", beta, epsilon)

	n := len(pages)
	idRemap := make(map[uint64]int, n)
	lastRank := make([]float64, n)
	curRank := make([]float64, n)

	for i := 0; i < n; i++ {
		idRemap[pages[i].Id] = int(i)
	}

	for iteration, delta := 1, math.MaxFloat64; delta > epsilon; iteration++ {
		curRank, lastRank = lastRank, curRank
		if iteration > 1 {
			for i := 0; i < n; i++ {
				curRank[i] = 0.0
			}
		} else {
			for i := 0; i < n; i++ {
				lastRank[i] = 1.0 / float64(n)
			}
		}

		// Single Random Walk Iteration
		for i := 0; i < n; i++ {
			contribution := beta * lastRank[i] / float64(len(pages[i].Links))
			for _, link := range pages[i].Links {
				curRank[idRemap[link.PageId]] += contribution
			}
		}

		// Reinsert leaked probability (why?  Shouldn't it already add to 1?)
		S := float64(0.0)
		for i := 0; i < n; i++ {
			S += curRank[i]
		}
		leakedRank := (1.0 - S) / float64(n)
		delta = 0.0
		for i := 0; i < n; i++ {
			curRank[i] += leakedRank
			delta += math.Abs(curRank[i] - lastRank[i])
		}

		log.Printf("Pagerank iteration #%d delta=%f", iteration, delta)
	}

	return curRank
}

func WriteRankedPages(outfile string, pageCount int, cp chan *RankedPage, done chan bool) {
	of, err := os.Create(outfile)
	if err != nil {
		panic(err)
	}
	defer of.Close()

	gobEncoder := gob.NewEncoder(of)
	gobEncoder.Encode(pageCount)

	for rp := range cp {
		if rp != nil {
			gobEncoder.Encode(rp)
		} else {
			log.Printf("Null page while writing ranked pages")
		}
	}
	done <- true
}

func RankPages(infile, outfile string) (err error) {
	n := ReadNumPages(infile)
	pageList := make([]Page, n)

	log.Printf("Reading '%d' pages from input file...", n)
	inputChan := make(chan *Page, 100)
	go ReadPages(infile, inputChan)

	i := 0
	for page := range inputChan {
		pageList[i] = *page
		i++
	}

	log.Printf("Computing PageRank...")
	rankVector := pageRank(pageList)

	log.Printf("Converting to ranked pages...")
	rankList := make([]RankedPage, n)
	for i := 0; i < n; i++ {
		page := &pageList[i]
		rankList[i] = RankedPage{
			Page: *page,
			Rank: float32(rankVector[i]),
		}
	}

	log.Printf("Calculating percentiles...")
	sort.Sort(RankedPageList(rankList))
	for i := 0; i < n; i++ {
		rankList[i].Order = uint32(i + 1)
	}

	log.Printf("Top 50 (Rank, Title, PageRank, NumLinks, NumAliases)")
	for i := 0; i < 50; i++ {
		log.Println(rankList[i].Order, rankList[i].Title, rankList[i].Rank, len(rankList[i].Links), len(rankList[i].Aliases))
	}

	log.Printf("Writing ranked pages...")
	outputChan := make(chan *RankedPage, 100)
	writeDoneChan := make(chan bool, 1)
	go WriteRankedPages(outfile, n, outputChan, writeDoneChan)
	for i := 0; i < n; i++ {
		outputChan <- &(rankList[i])
	}
	close(outputChan)
	<-writeDoneChan

	log.Printf("Done writing pages")
	return
}
