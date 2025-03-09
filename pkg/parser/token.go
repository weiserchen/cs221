package parser

import (
	"html"
	"regexp"
	"strings"
	"unicode"

	xhtml "golang.org/x/net/html"

	"github.com/microcosm-cc/bluemonday"
	"github.com/surgebase/porter2"
)

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
		if unicode.IsPunct(r) {
			sb.WriteRune(' ')
		} else {
			sb.WriteRune(r)
		}
	}
	content = sb.String()
	content = strings.ToLower(content)
	return content
}

func ParseTokens(s string) []string {
	// re := regexp.MustCompile(`[a-z0-9]+`)
	// tokens := re.FindAllString(s, -1)
	// tokenStr := strings.Join(tokens, " ")

	// tokens := []string{}

	// stream := jargon.TokenizeString(s).
	// 	Filter(stackoverflow.Tags).
	// 	Filter(contractions.Expand)

	// for stream.Scan() {
	// 	tokens = append(tokens, strings.TrimSpace(stream.Token().String()))
	// }

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

func StemTokens(tokens []string) []string {
	stemmed := make([]string, 0, len(tokens))
	for _, token := range tokens {
		stemmed = append(stemmed, porter2.Stem(token))
	}
	return stemmed
}

func TwoGrams(tokens []string) []string {
	twoGrams := []string{}
	for i := 0; i < len(tokens)-1; i++ {
		twoGrams = append(twoGrams, strings.Join([]string{tokens[i], tokens[i+1]}, "+"))
	}
	return twoGrams
}

func ThreeGrams(tokens []string) []string {
	threeGrams := []string{}
	for i := 0; i < len(tokens)-2; i++ {
		threeGrams = append(threeGrams, strings.Join([]string{tokens[i], tokens[i+1], tokens[i+2]}, "+"))
	}
	return threeGrams
}

func FourGrams(tokens []string) []string {
	fourGrams := []string{}
	for i := 0; i < len(tokens)-3; i++ {
		fourGrams = append(fourGrams, strings.Join([]string{tokens[i], tokens[i+1], tokens[i+2], tokens[i+3]}, "+"))
	}
	return fourGrams
}
