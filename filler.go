package axq

type Filler interface {
}

type FillerBuilder struct {
}

func NewFiller() *FillerBuilder {
	return &FillerBuilder{}
}
