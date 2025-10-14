package job

import (
	"fmt"
	"go.mongodb.org/mongo-driver/v2/bson"
	"time"
)

// @tpm-schematics:start-region("top-file-section")
// @tpm-schematics:end-region("top-file-section")

type QueryOptions struct {
	Limit         int64  `form:"limit" query:"limit" json:"limit,omitempty" bson:"limit,omitempty" yaml:"limit,omitempty"`
	Offset        int64  `form:"offset" query:"offset" json:"offset,omitempty" bson:"offset,omitempty" yaml:"offset,omitempty"`
	SortBy        string `form:"sortBy" query:"sortBy" json:"sortBy,omitempty" bson:"sortBy,omitempty" yaml:"sortBy,omitempty"`
	SortDirection string `form:"sortDirection" query:"sortDirection" json:"sortDirection,omitempty" bson:"sortDirection,omitempty" yaml:"sortDirection,omitempty"`
	SearchTerm    string `form:"ssearch" query:"ssearch" json:"ssearch,omitempty" bson:"ssearch,omitempty" yaml:"ssearch,omitempty"`
	WithCount     bool   `form:"withCount" query:"withCount" json:"withCount,omitempty" bson:"withCount,omitempty" yaml:"withCount,omitempty"`
	// @tpm-schematics:start-region("query-options-struct-section")
	// @tpm-schematics:end-region("query-options-struct-section")
}

type SortOptions struct {
	document bson.D
}

func NewSortOptions(opts ...SortOption) SortOptions {
	sops := SortOptions{}
	for _, o := range opts {
		o(&sops)
	}

	return sops
}

func (pops SortOptions) Build() bson.D {
	return pops.document
}

type SortOption func(sops *SortOptions)

func WithSortByFieldAsc(fn string) SortOption {
	return func(sops *SortOptions) {
		sops.document = append(sops.document, bson.E{Key: fn, Value: 1})
	}
}

func WithSortByFieldDesc(fn string) SortOption {
	return func(sops *SortOptions) {
		sops.document = append(sops.document, bson.E{Key: fn, Value: -1})
	}
}

func WithSortByTextSearchRankingDesc(fn string) SortOption {
	return func(sops *SortOptions) {
		sops.document = append(sops.document, bson.E{Key: fn, Value: bson.E{Key: "$meta", Value: "textScore"}})
	}
}

func CriteriaGoInfo() string {
	i := fmt.Sprintf("tpm_morphia query filter support generated for %s package on %s", "author", time.Now().String())
	return i
}

type ProjectionOptions struct {
	document bson.D
}

type ProjectionOption func(sops *ProjectionOptions)

func (pops ProjectionOptions) Build() bson.D {
	return pops.document
}

func WithNoId() ProjectionOption {
	return func(sops *ProjectionOptions) {
		sops.document = append(sops.document, bson.E{Key: "_id", Value: 0})
	}
}

func WithTextSearchRanking(fn string) ProjectionOption {
	return func(sops *ProjectionOptions) {
		sops.document = append(sops.document, bson.E{Key: fn, Value: bson.E{Key: "$meta", Value: "textScore"}})
	}
}

func WithIncludeField(fn string) ProjectionOption {
	return func(sops *ProjectionOptions) {
		sops.document = append(sops.document, bson.E{Key: "fn", Value: 1})
	}
}

func WithExcludeField(fn string) ProjectionOption {
	return func(sops *ProjectionOptions) {
		sops.document = append(sops.document, bson.E{Key: "fn", Value: 0})
	}
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
