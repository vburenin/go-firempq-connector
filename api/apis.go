package api

type ITokenReader interface {
	ReadTokens() ([]string, error)
}
