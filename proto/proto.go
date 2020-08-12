package proto

//go:generate protoc --go_out=. --go_opt=paths=source_relative natsrpc.proto

func (err *Error) Error() string {
	return err.Message
}
