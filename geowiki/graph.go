package geowiki

import (
	"compress/bzip2"
	"compress/gzip"
	"encoding/gob"
	"encoding/xml"
	"io"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
)

type redirect struct {
	Title string `xml:"title,attr"`
}

type pageElement struct {
	Title    string   `xml:"title"`
	Redirect redirect `xml:"redirect"`
	Text     string   `xml:"revision>text"`
	Id       uint64   `xml:"id"`
}

type Link struct {
	PageId uint64
	Count  uint32
}

type Page struct {
	Title   string
	Id      uint64
	Aliases []string
	Links   []Link
}

// ignore pages with titles matching this pattern
var titleFilter = regexp.MustCompile("^(File|Talk|Special|Wikipedia|Wiktionary|User|User Talk|Category|Portal|Template|Mediawiki|Help|Draft):")

var cleanSectionRegex = regexp.MustCompile(`^[^#]*`)
var linkRegex = regexp.MustCompile(`\[\[(?:([^|\]]*)\|)?([^\]]+)\]\]`)

func yieldPageElements(infile string, cp chan *pageElement) {
	defer close(cp)

	f, err := os.Open(infile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var xmlReader io.Reader
	if strings.HasSuffix(infile, ".bz2") {
		log.Printf("Assuming bzip2 compressed dump")
		xmlReader = bzip2.NewReader(f)
	} else if strings.HasSuffix(infile, ".gz") {
		log.Printf("Assuming gzip compressed dump")
		xmlReader, err = gzip.NewReader(f)
	} else {
		log.Printf("Assuming uncompressed dump")
		xmlReader = f
	}
	if err != nil {
		panic(err)
	}

	pageCount := 0
	log.Printf("Starting parse")
	decoder := xml.NewDecoder(xmlReader)
	for {
		token, err := decoder.Token()
		if err == io.EOF || token == nil {
			log.Printf("EOF")
			break
		} else if err != nil {
			log.Panic(err)
		}

		switch e := token.(type) {
		case xml.StartElement:
			switch e.Name.Local {
			case "page":
				var p pageElement
				decoder.DecodeElement(&p, &e)
				if titleFilter.MatchString(p.Title) {
					continue
				}
				pageCount++
				if pageCount%100000 == 0 {
					log.Printf("Reached page %d", pageCount)
				}
				cp <- &p
			case "mediawiki":
			default:
				decoder.Skip()
			}
		default:
		}
	}
}

func WritePages(outfile string, numPages int, cp chan *Page, done chan bool) {
	of, err := os.Create(outfile)
	if err != nil {
		panic(err)
	}
	defer of.Close()

	gobEncoder := gob.NewEncoder(of)
	gobEncoder.Encode(numPages)

	for page := range cp {
		if page != nil {
			err = gobEncoder.Encode(page)
			if err != nil {
				panic(err)
			}
		} else {
			log.Printf("Null page while writing pages")
		}
	}
	done <- true
}

func Graph(infile, locationsfile, outfile string) (err error) {
	pages := make([]*Page, 0, 5000000)
	pageTitleMap := make(map[string]*Page, 12000000)

	log.Printf("Starting pass 1: pages")
	pageInputChan := make(chan *pageElement, 20000)
	go yieldPageElements(infile, pageInputChan)
	for pe := range pageInputChan {
		if len(pe.Redirect.Title) == 0 {
			p := &Page{
				Title: pe.Title,
				Id:    pe.Id,
				Links: make([]Link, 0, 10),
			}

			pages = append(pages, p)
			pageTitleMap[pe.Title] = p
		}
	}

	log.Printf("Starting pass 2: redirects")
	pageInputChan = make(chan *pageElement, 1000)
	go yieldPageElements(infile, pageInputChan)
	for pe := range pageInputChan {
		if len(pe.Redirect.Title) > 0 {
			redirectedTitle := cleanSectionRegex.FindString(pe.Redirect.Title)
			if redirectPage, ok := pageTitleMap[redirectedTitle]; ok {
				redirectPage.Aliases = append(redirectPage.Aliases, pe.Title)
				pageTitleMap[pe.Title] = redirectPage
			} else if !titleFilter.MatchString(redirectedTitle) {
				log.Printf("Unresolvable redirect: '%s' -> '%s' (cleaned '%s')", pe.Title, pe.Redirect.Title, redirectedTitle)
			}
		}
	}

	log.Printf("Starting pass 3: links")
	pageInputChan = make(chan *pageElement, 1000)
	go yieldPageElements(infile, pageInputChan)
	for pe := range pageInputChan {
		fromPage, ok := pageTitleMap[pe.Title]
		if !ok {
			if pe.Redirect.Title != "" {
				log.Printf("Warning: page '%s' not in title map", pe.Title)
			}
			continue
		}

		linkCounts := make(map[uint64]uint32)
		submatches := linkRegex.FindAllStringSubmatch(pe.Text, -1)
		for _, submatch := range submatches {
			var dirtyLinkName string
			if len(submatch[1]) == 0 {
				dirtyLinkName = submatch[2]
			} else {
				dirtyLinkName = cleanSectionRegex.FindString(submatch[1])
			}

			toPage, ok := pageTitleMap[dirtyLinkName]
			if ok && toPage.Id != fromPage.Id {
				linkCounts[toPage.Id]++
			}
		}

		for linkedId, count := range linkCounts {
			fromPage.Links = append(fromPage.Links, Link{
				PageId: linkedId,
				Count:  count,
			})
		}
	}

	log.Printf("Starting writing...")
	pageOutputChan := make(chan *Page, 1000)
	writeDoneChan := make(chan bool)
	go WritePages(outfile, len(pages), pageOutputChan, writeDoneChan)
	for _, p := range pages {
		sort.Strings(p.Aliases)
		pageOutputChan <- p
	}
	close(pageOutputChan)
	<-writeDoneChan

	log.Printf("Done writing")

	return
}
