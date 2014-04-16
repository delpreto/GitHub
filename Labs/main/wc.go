package main

import "os"
import "fmt"
import "mapreduce"
import "container/list"
import "strconv"

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
// TODO: just saw hint about strings.FieldsFunc - look into using this instead?
func Map(value string) *list.List {
	wordList := list.New()
	// Iterate through all words in the document
	i := 0
	for i < len(value) {
		// Get to the start of a word
		// Note that a word is hereby required to start with a letter of the alphabet
		//   (for example, no contractions with apostraphe in front, which doesn't exist in the sample text anyway)
		isWordCharacter := false
		for i < len(value) && !isWordCharacter {
			if value[i] >= 'a' && value[i] <= 'z' {
				isWordCharacter = true
			}
			if value[i] >= 'A' && value[i] <= 'Z' {
				isWordCharacter = true
			}
			if !isWordCharacter {
				i++
			}
		}
		startWord := i // Record index at which word starts
		// Read the whole word
		//   Words can be hyphenated or include apostrophes (possessives, contractions)
		for i < len(value) && isWordCharacter {
			isWordCharacter = false
			if value[i] >= 'a' && value[i] <= 'z' {
				isWordCharacter = true
			}
			if value[i] >= 'A' && value[i] <= 'Z' {
				isWordCharacter = true
			}
			if value[i] == '-' || value[i] == '\'' {
				isWordCharacter = true
			}
			if isWordCharacter {
				i++
			}
		}
		currentWord := value[startWord:i]
		// Trim any trailing hyphens (trailing apostrophes are OK)
		hyphenIndex := len(currentWord)
		for hyphenIndex-1 >= 0 && currentWord[hyphenIndex-1] == '-' {
			hyphenIndex--
		}
		// If we found any trailing hyphens, remove them
		//    (note: the entire word can't be hyphens since we restricted words to start with a letter)
		if hyphenIndex < len(currentWord) {
			currentWord = currentWord[0:hyphenIndex]
		}
		wordList.PushBack(mapreduce.KeyValue{currentWord, "1"})
	}
	return wordList
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
	wordCount := 0
	// Iterate through list of values to sum the counts
	for e := values.Front(); e != nil; e = e.Next() {
		count, err := strconv.Atoi(e.Value.(string))
		if err == nil {
			wordCount += count
		} else {
			fmt.Println("ERROR CONVERTING STRING TO INT WHEN VALUE = ", e.Value)
		}
	}
	return strconv.Itoa(wordCount)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) != 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		if os.Args[3] == "sequential" {
			mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
		} else {
			mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])
			// Wait until MR is done
			<-mr.DoneChannel
		}
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
	}
}
