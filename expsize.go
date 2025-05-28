package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ulikunitz/xz"
)

type objectStats struct {
	sum   int64
	count int64
	min   int64
	max   int64
}

func (o *objectStats) mean() int64 {
	if o.count == 0 {
		return 0
	}
	return o.sum / o.count
}

func main() {
	file := flag.String("file", "", "text dump of s3 files")
	after := flag.String("after", "", "ignore files before this date (YYYY-MM-DD)")
	before := flag.String("before", "", "ignore files after this date (YYYY-MM-DD)")
	flag.Parse()

	f, err := os.Open(*file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var cutoffBefore time.Time
	if *after != "" {
		cutoffBefore, err = time.Parse("2006-01-02", *after)
		if err != nil {
			log.Fatalf("Invalid date format for --after: %v", err)
		}
	}

	var cutoffAfter time.Time
	if *before != "" {
		cutoffAfter, err = time.Parse("2006-01-02", *before)
		if err != nil {
			log.Fatalf("Invalid date format for --before: %v", err)
		}
	}

	// Wrap the file with an XZ reader (streams & checks CRCs for you).
	r, err := xz.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}

	object := make(map[string]*objectStats, 500)

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		col := strings.Fields(scanner.Text())

		date, _ := time.Parse("2006/01/02", col[0])
		if *after != "" && date.Before(cutoffBefore) {
			// fmt.Printf("Skipping %s %s, before cutoff date %s\n", col[0], date, cutoffBefore.Format("2006-01-02"))
			continue
		}
		if *before != "" && date.After(cutoffAfter) {
			// fmt.Printf("Skipping %s %s, after cutoff date %s\n", col[0], date, cutoffAfter.Format("2006-01-02"))
			continue
		}

		size, _ := strconv.ParseInt(col[2], 10, 64)
		// remove the first 52 characters from the filename
		name := col[3][51:]

		current, ok := object[name]
		if !ok {
			// first time this filename has been seen
			current = &objectStats{}
			object[name] = current
		}

		current.count++
		current.sum += size

		if size < current.min || current.min == 0 {
			// new min size for this object
			current.min = size
		}

		if size > current.max {
			// new max size for this object
			current.max = size
		}

		// fmt.Printf("name %q %v\n", name, current)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// create a slice of keys that can be sorted
	keys := make([]string, 0, len(object))
	for k := range object {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	totalSize := int64(0)
	for _, name := range keys {
		o := object[name]
		fmt.Printf("%s max: %d mean: %d min: %d\n", name, o.max, o.mean(), o.min)
		totalSize += object[name].max
	}

	fmt.Printf("Total number of files: %d\n", len(object))
	fmt.Printf("Total size of all files: %d bytes\n", totalSize)
}
