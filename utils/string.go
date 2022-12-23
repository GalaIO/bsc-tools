package utils

import (
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

func ParseInt64(s string) int64 {
	r, err := strconv.ParseInt(s, 10, 64)
	PanicErr(err)
	return r
}

func ReadLines(path string) []string {
	tmpFile, err := os.Open(path)
	PanicErr(err)
	defer tmpFile.Close()
	tmpBytes, err := ioutil.ReadAll(tmpFile)
	PanicErr(err)
	return strings.Split(string(tmpBytes), "\n")
}

func WriteLines(path string, lines []string) {
	os.Remove(path)
	rewardFile, err := os.Create(path)
	PanicErr(err)
	defer rewardFile.Close()
	rewardFile.Write([]byte(strings.Join(lines, "\n")))
}

func ReadString(path string) string {
	tmpFile, err := os.Open(path)
	PanicErr(err)
	defer tmpFile.Close()
	tmpBytes, err := ioutil.ReadAll(tmpFile)
	PanicErr(err)
	return string(tmpBytes)
}

func WriteString(path string, str string) {
	os.Remove(path)
	f, err := os.Create(path)
	PanicErr(err)
	defer f.Close()
	f.WriteString(str)
}

func FindMatch(s string, subs []string) int {
	for i := range subs {
		if strings.Contains(s, subs[i]) {
			return i
		}
	}

	return -1
}
