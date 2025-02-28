package lease

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"time"
)

// @tpm-schematics:start-region("top-file-section")
// @tpm-schematics:end-region("top-file-section")

func CriteriaGoInfo() string {
	i := fmt.Sprintf("tpm_morphia query filter support generated for %s package on %s", "author", time.Now().String())
	return i
}

const (
	Text                   string = "$text"
	Regex                  string = "$regex"
	TextLanguage           string = "$language"
	TextCaseSensitive             = "$caseSensitive"
	TextDiacriticSensitive        = "$diacriticSensitive"
	TextMeta                      = "$meta"
	TextMetaTextScore             = "textScore"
)

type Criterion func() bson.E
type Criteria []Criterion

type Filter struct {
	listOfCriteria []Criteria
}

func (f *Filter) Or() *Criteria {
	ca := make(Criteria, 0)

	/*
		 * Note: the array gets allocated to avoid some nasty problem related to the fact that the underlying array can be moved
		 * when needs to make room. In this case if somebody write something like this:

			f = Filter{}
			ca := f.or().lastNameEqTo("Smith").firstNameEqTo("Colin")
			f.or().firstNameEqTo("John")
			ca.books().titleEq("My Best Seller")

		 * and saves the variable ca to be reused what happens is that the second f.or() causes the listOfCriteria
		 * array to be moved and that slice is dead as to speak.
		 * Anyway, best option would be not to interleave the conditions between or() calls.
	*/
	if len(f.listOfCriteria) == 0 {
		f.listOfCriteria = make([]Criteria, 0, 5)
	}

	// Should check top criteria if empty...
	f.listOfCriteria = append(f.listOfCriteria, ca)
	return &f.listOfCriteria[len(f.listOfCriteria)-1]
}

var emptyFilter = bson.D{}

func (f *Filter) Build() bson.D {

	if len(f.listOfCriteria) == 0 {
		return emptyFilter
	}

	docA := bson.A{}
	for _, cas := range f.listOfCriteria {
		doc := bson.D{}
		for _, c := range cas {
			doc = append(doc, c())
		}
		docA = append(docA, doc)
	}

	if len(docA) == 1 {
		return docA[0].(bson.D)
	}

	return bson.D{{"$or", docA}}
}

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
