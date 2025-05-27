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

	"github.com/ulikunitz/xz"
)

func main() {
	file := flag.String("file", "", "text dump of s3 files")
	flag.Parse()

	f, err := os.Open(*file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Wrap the file with an XZ reader (streams & checks CRCs for you).
	r, err := xz.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}

	maxSize := make(map[string]int64, 500)

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		col := strings.Fields(scanner.Text())
		size, _ := strconv.ParseInt(col[2], 10, 64)
		// remove the first 52 characters from the filename
		name := col[3][51:]

		current, ok := maxSize[name]
		if !ok {
			// first time this filename has been seen
			maxSize[name] = size
		}

		if size > current {
			// new maximum size for this filename
			maxSize[name] = size
		}

		// fmt.Printf("name %q, size %d\n", name, size)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// create a slice of keys that can be sorted
	keys := make([]string, 0, len(maxSize))
	for k := range maxSize {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	totalSize := int64(0)
	for _, name := range keys {
		fmt.Printf("%s %d\n", name, maxSize[name])
		totalSize += maxSize[name]
	}

	fmt.Printf("Total number of files: %d\n", len(maxSize))
	fmt.Printf("Total size of all files: %d bytes\n", totalSize)
}
