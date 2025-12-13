package protocol

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
)

type Client struct {
	conn    net.Conn
	scanner *bufio.Scanner
}

func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:    conn,
		scanner: bufio.NewScanner(conn),
	}, nil
}

func (c *Client) Close() {
	c.conn.Close()
}

func (c *Client) SendRequest(req Request) (Response, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return Response{}, err
	}

	_, err = c.conn.Write(append(data, '\n'))
	if err != nil {
		return Response{}, fmt.Errorf("failed to send request: %w", err)
	}

	if !c.scanner.Scan() {
		return Response{}, fmt.Errorf("failed to read response: %w", c.scanner.Err())
	}

	var resp Response
	if err := json.Unmarshal(c.scanner.Bytes(), &resp); err != nil {
		return Response{}, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return resp, nil
}
