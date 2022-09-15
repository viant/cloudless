package sync

//CSVUnmarshaler represents CSV Unmarshaler
type CSVUnmarshaler interface {
	UnmarshalCSV(text string) error
	SkipHeader() bool
}
