package parser

import (
	"fmt"
	"html"
	"regexp"
	"strings"
	"unicode"

	xhtml "golang.org/x/net/html"

	"github.com/microcosm-cc/bluemonday"
)

type Doc struct {
	Tokens []string
	TagMap map[string][]string
}

func (doc *Doc) Print() {
	fmt.Println(doc.Tokens)
	for tag, list := range doc.TagMap {
		fmt.Printf("%s: %v\n", tag, list)
	}
}

func ParseDoc(s string) Doc {
	content := Sanitize(s)
	tokens := ParseTokens(content)
	tagMap := ExtractTagMap(s)
	return Doc{
		Tokens: tokens,
		TagMap: tagMap,
	}
}

func ExtractTagMap(s string) map[string][]string {
	count := map[string][]string{}
	doc, _ := xhtml.Parse(strings.NewReader(s))

	var crawl func(node *xhtml.Node)
	crawl = func(node *xhtml.Node) {
		if node.Type == xhtml.ElementNode {
			tag := node.Data
			switch node.Data {
			case "title", "h1", "h2", "h3":
				if node.FirstChild != nil && node.FirstChild.Type == xhtml.TextNode {
					content := strings.Join(strings.Fields(node.FirstChild.Data), " ")
					count[tag] = append(count[tag], content)
				}

			case "b", "strong":
				if node.FirstChild != nil && node.FirstChild.Type == xhtml.TextNode {
					content := strings.Join(strings.Fields(node.FirstChild.Data), " ")
					count["b"] = append(count["b"], content)
				}
			case "i", "em":
				if node.FirstChild != nil && node.FirstChild.Type == xhtml.TextNode {
					content := strings.Join(strings.Fields(node.FirstChild.Data), " ")
					count["i"] = append(count["i"], content)
				}
			}
		}

		for c := node.FirstChild; c != nil; c = c.NextSibling {
			crawl(c)
		}
	}

	crawl(doc)
	return count
}

func Sanitize(s string) string {
	p := bluemonday.StripTagsPolicy()
	content := p.Sanitize(s)
	content = html.UnescapeString(content)
	var sb strings.Builder
	for _, r := range content {
		if !unicode.IsPunct(r) {
			sb.WriteRune(r)
		}
	}
	content = sb.String()
	content = strings.ToLower(content)
	return content
}

func ParseTokens(s string) []string {
	re := regexp.MustCompile(`[a-z0-9]+`)
	tokens := re.FindAllString(s, -1)
	return tokens
}

func ParseTagMap(tagMap map[string][]string) map[string][]string {
	tokenMap := map[string][]string{}
	for tag, list := range tagMap {
		for _, v := range list {
			tokenMap[tag] = append(tokenMap[tag], ParseTokens(Sanitize(v))...)
		}
	}
	return tokenMap
}
