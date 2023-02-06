package shared

type Error struct {
	Type       string    `json:"errorType,omitempty"`
	Message    string    `json:"errorMessage"`
	StackTrace []*string `json:"stackTrace,omitempty"`
	Cause      *Error    `json:"cause,omitempty"`
}
