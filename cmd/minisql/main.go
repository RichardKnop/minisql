package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/RichardKnop/minisql/internal/core/minisql"
	"github.com/RichardKnop/minisql/internal/core/parser"
	"github.com/RichardKnop/minisql/internal/core/protocol"
	"github.com/RichardKnop/minisql/internal/pkg/logging"
)

const defaultDbFileName = "db"

var (
	dbNameFlag string
	portFlag   int
)

func init() {
	flag.StringVar(&dbNameFlag, "db", defaultDbFileName, "Filename of the database to use")
	flag.IntVar(&portFlag, "port", 8080, "Port to listen on")
}

type Server struct {
	listener net.Listener
	database *minisql.Database
	quit     chan struct{}
	wg       sync.WaitGroup
	logger   *zap.Logger

	// Add connection tracking
	connections map[minisql.ConnectionID]*minisql.Connection
	nextConnID  minisql.ConnectionID
	connMu      sync.RWMutex
}

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logConf := logging.DefaultConfig()

	level := os.Getenv("LOG_LEVEL")
	if level == "" {
		level = "info"
	}

	l, err := logging.ParseLevel(level)
	if err != nil {
		panic(err)
	}
	logConf.Level = zap.NewAtomicLevelAt(l)

	logger, err := logConf.Build()
	if err != nil {
		panic(err)
	}
	defer logger.Sync() // flushes buffer, if any

	dbFile, err := os.OpenFile(dbNameFlag, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer dbFile.Close()

	aPager, err := minisql.NewPager(dbFile, minisql.PageSize)
	if err != nil {
		panic(err)
	}
	aDatabase, err := minisql.NewDatabase(ctx, logger, "db", parser.New(), aPager, aPager, aPager)
	if err != nil {
		panic(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	aServer := &Server{
		database:    aDatabase,
		quit:        make(chan struct{}),
		connections: make(map[minisql.ConnectionID]*minisql.Connection),
		logger:      logger,
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", portFlag))
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	logger.Info("listening on port", zap.Int("port", portFlag))

	aServer.listener = listener
	aServer.wg.Add(1)

	go aServer.serve(ctx)

	<-sigChan

	aServer.stop()
	if err := aDatabase.Close(ctx); err != nil {
		fmt.Printf("error closing database: %s\n", err)
	}
	cancel()

	os.Exit(0)
}

func (s *Server) serve(ctx context.Context) {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				log.Println("accept error", err)
			}
		} else {
			s.wg.Add(1)
			go func(tcpConn net.Conn) {
				defer s.wg.Done()

				// Create connection context
				s.connMu.Lock()
				s.nextConnID++
				aConnection := s.database.NewConnection(s.nextConnID, tcpConn)
				s.connections[aConnection.ID] = aConnection
				s.connMu.Unlock()

				s.logger.Debug("new connection", zap.String("id", fmt.Sprint(aConnection.ID)))

				// Handle connection messages
				s.handleConnection(ctx, aConnection)

				// Cleanup on disconnect
				s.connMu.Lock()
				delete(s.connections, aConnection.ID)
				s.connMu.Unlock()

				s.logger.Debug("connection closed", zap.String("id", fmt.Sprint(aConnection.ID)))
			}(conn)
		}
	}
}

func (s *Server) stop() {
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

func (s *Server) handleConnection(ctx context.Context, conn *minisql.Connection) {
	defer conn.Close()
	defer conn.Cleanup(ctx)

	buf := make([]byte, 2048)

ReadLoop:
	for {
		select {
		case <-s.quit:
			return
		default:
			conn.TcpConn().SetDeadline(time.Now().Add(200 * time.Millisecond))
			n, err := conn.TcpConn().Read(buf)
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					continue ReadLoop
				} else if err != io.EOF {
					s.logger.Error("read error", zap.Error(err))
					return
				}
			}
			if n == 0 {
				return
			}

			if err := s.handleMessage(ctx, conn, buf[:n]); err != nil {
				s.logger.Error("error handling message", zap.Error(err))
				return
			}
		}
	}
}

func (s *Server) handleMessage(ctx context.Context, conn *minisql.Connection, msg []byte) error {
	s.logger.Debug("Received message", zap.String("message", string(msg)))

	var req protocol.Request
	if err := json.Unmarshal(msg, &req); err != nil {
		return s.sendResponse(conn, protocol.Response{
			Success: false,
			Error:   fmt.Sprintf("Invalid JSON: %v", err),
		})
	}

	switch req.Type {
	case "ping":
		s.sendResponse(conn, protocol.Response{
			Success: true,
			Message: "pong",
		})
	case "list_tables":
		tableNames := s.database.ListTableNames(ctx)
		return s.sendResponse(conn, protocol.Response{
			Success: true,
			Message: strings.Join(tableNames, "\n"),
		})
	case "sql":
		return s.handleSQL(ctx, conn, req.SQL)
	default:
		return s.sendResponse(conn, protocol.Response{
			Success: false,
			Error:   fmt.Sprintf("Unknown request type: %s", req.Type),
		})
	}

	return nil
}

func (s *Server) handleSQL(ctx context.Context, conn *minisql.Connection, sql string) error {
	stmts, err := s.database.PrepareStatements(ctx, sql)
	if err != nil {
		return s.sendResponse(conn, protocol.Response{
			Success: false,
			Error:   fmt.Sprintf("Parse error: %v", err),
		})
	}

	for _, stmt := range stmts {
		results, err := conn.ExecuteStatements(ctx, stmt)
		if err != nil {
			return s.sendResponse(conn, protocol.Response{
				Success: false,
				Error:   err.Error(),
			})
		}
		if len(results) == 0 {
			continue
		}
		aResult := results[0]

		aResponse := protocol.Response{
			Kind:         stmt.Kind,
			Success:      true,
			Columns:      aResult.Columns,
			Rows:         make([][]minisql.OptionalValue, 0, 10),
			RowsAffected: aResult.RowsAffected,
		}

		if aResult.Rows != nil {
			aRow, err := aResult.Rows(ctx)
			for ; err == nil; aRow, err = aResult.Rows(ctx) {
				// Convert TextPointer structs to strings
				// TODO - find less hacky way to do this
				values := make([]minisql.OptionalValue, 0, len(aRow.Values))
				for _, aValue := range aRow.Values {
					if !aValue.Valid {
						values = append(values, aValue)
						continue
					}
					textPointer, ok := aValue.Value.(minisql.TextPointer)
					if !ok {
						values = append(values, aValue)
						continue
					}
					values = append(values, minisql.OptionalValue{
						Value: textPointer.String(),
						Valid: true,
					})
				}
				aResponse.Rows = append(aResponse.Rows, values)
			}
		}

		if err := s.sendResponse(conn, aResponse); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) sendResponse(conn *minisql.Connection, resp protocol.Response) error {
	jsonData, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("error marshalling response: %v", err)
	}
	_, err = conn.TcpConn().Write(jsonData)
	if err != nil {
		return fmt.Errorf("error writing response: %v", err)
	}
	_, err = conn.TcpConn().Write([]byte("\n"))
	if err != nil {
		return fmt.Errorf("error writing response newline: %v", err)
	}
	return nil
}
