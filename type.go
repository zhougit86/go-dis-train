package main

import(
	"fmt"
	"strings"
	"unicode"
	"strconv"
)

type KeyValue struct {
	Key   string
	Value string
}

const sample = "\xbd\xb2\x3d\xbc\x20\xe2\x8c\x98"

func main()  {
	a:=[]byte{'c','i','s','c','o','1'}
	for _,v:=range (a){
		fmt.Println(unicode.IsLetter(rune(v)))
	}
	fmt.Println(string(a))
	fmt.Println(strings.Fields(string(a)))


	b:=34077
	fmt.Println(string(b),strconv.Itoa(b))

	c:=[]byte{'a','\n','b'}
	fmt.Println(c,string(c))
	for i := 0; i < len(sample); i++ {
		fmt.Printf("%x ", sample[i])
	}
	const placeOfInterest = `⌘`

	fmt.Printf("plain string: ")
	fmt.Printf("%s", placeOfInterest)
	fmt.Printf("\n")

	fmt.Printf("quoted string: ")
	fmt.Printf("%+q", placeOfInterest)
	fmt.Printf("\n")

	fmt.Printf("hex bytes: ")
	for i := 0; i < len(placeOfInterest); i++ {
		fmt.Printf("%x ", placeOfInterest[i])
	}
	fmt.Printf("\n")
}
