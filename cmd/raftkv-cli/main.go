package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"raftkv/internal/observability"
	clientv1 "raftkv/proto/client/v1"
)

func generateRequestID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func printUsage() {
	fmt.Println("Usage: raftkv-cli [flags] <command> [arguments]")
	fmt.Println("\nCommands:")
	fmt.Println("  get <key>              Get the value for a key")
	fmt.Println("  set <key> <value>      Set a key to a value")
	fmt.Println("  delete <key>           Delete a key")
	fmt.Println("  status                 Get cluster status")
	fmt.Println("  events replay <file>   Replay structured telemetry from JSON fixture")
	fmt.Println("  events live [http-url] Stream recent events from live node (/debug/events)")
	fmt.Println("\nFlags:")
	fmt.Println("  -server string         Server address (default \"localhost:50051\")")
	fmt.Println("  -timeout duration      Request timeout (default 5s)")
	fmt.Println("  -request-id string     Custom request ID for writes")
}

func main() {
	fs := flag.NewFlagSet("raftkv-cli", flag.ExitOnError)
	serverAddr := fs.String("server", "localhost:50051", "server address")
	timeout := fs.Duration("timeout", 5*time.Second, "request timeout")
	customReqID := fs.String("request-id", "", "custom request ID")

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	// Parse flags that may come before or after subcommand
	if err := fs.Parse(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error parsing flags: %v\n", err)
		os.Exit(1)
	}

	args := fs.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	cmd := args[0]

	if cmd == "events" {
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "Usage: raftkv-cli events <replay|live> [args]")
			os.Exit(1)
		}
		subcmd := args[1]
		switch subcmd {
		case "replay":
			if len(args) < 3 {
				fmt.Fprintln(os.Stderr, "Usage: raftkv-cli events replay <fixture.json>")
				os.Exit(1)
			}
			filePath := args[2]
			events, err := observability.LoadFromJSON(filePath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to load fixture: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("Loaded %d events from %s:\n", len(events), filePath)
			for _, e := range events {
				if err := observability.ValidateEvent(e); err != nil {
					fmt.Printf("[INVALID] %v\n", err)
					continue
				}
				corr := ""
				if e.CorrelationID != "" {
					corr = fmt.Sprintf(" corr=%s", e.CorrelationID)
				}
				peer := ""
				if e.PeerID != "" {
					peer = fmt.Sprintf(" peer=%s", e.PeerID)
				}
				fields := ""
				if len(e.Fields) > 0 {
					var pairs []string
					for k, v := range e.Fields {
						pairs = append(pairs, fmt.Sprintf("%s=%s", k, v))
					}
					fields = fmt.Sprintf(" [%s]", strings.Join(pairs, " "))
				}
				fmt.Printf("[%05d] %s %-18s node=%s%s%s term=%d idx=%d%s\n",
					e.Sequence, e.Timestamp.Format("15:04:05.000"), e.Type, e.NodeID, peer, corr, e.Term, e.LogIndex, fields)
			}
			return
		case "live":
			httpAddr := "http://localhost:9091/debug/events"
			if len(args) >= 3 {
				httpAddr = args[2]
				if !strings.HasPrefix(httpAddr, "http://") && !strings.HasPrefix(httpAddr, "https://") {
					httpAddr = "http://" + httpAddr + "/debug/events"
				}
			}
			resp, err := http.Get(httpAddr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to fetch live events from %s: %v\n", httpAddr, err)
				os.Exit(1)
			}
			defer resp.Body.Close()
			var events []observability.ClusterEvent
			if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
				fmt.Fprintf(os.Stderr, "failed to decode events: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("Live events from %s (%d events):\n", httpAddr, len(events))
			for _, e := range events {
				corr := ""
				if e.CorrelationID != "" {
					corr = fmt.Sprintf(" corr=%s", e.CorrelationID)
				}
				peer := ""
				if e.PeerID != "" {
					peer = fmt.Sprintf(" peer=%s", e.PeerID)
				}
				fields := ""
				if len(e.Fields) > 0 {
					var pairs []string
					for k, v := range e.Fields {
						pairs = append(pairs, fmt.Sprintf("%s=%s", k, v))
					}
					fields = fmt.Sprintf(" [%s]", strings.Join(pairs, " "))
				}
				fmt.Printf("[%05d] %s %-18s node=%s%s%s term=%d idx=%d%s\n",
					e.Sequence, e.Timestamp.Format("15:04:05.000"), e.Type, e.NodeID, peer, corr, e.Term, e.LogIndex, fields)
			}
			return
		default:
			fmt.Fprintf(os.Stderr, "unknown events subcommand %q\n", subcmd)
			os.Exit(1)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, *serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to server at %s: %v\n", *serverAddr, err)
		os.Exit(1)
	}
	defer conn.Close()

	client := clientv1.NewClientServiceClient(conn)

	switch cmd {
	case "get":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "Usage: raftkv-cli get <key>")
			os.Exit(1)
		}
		key := args[1]
		resp, err := client.Get(ctx, &clientv1.GetRequest{Key: key})
		if err != nil {
			fmt.Fprintf(os.Stderr, "RPC failed: %v\n", err)
			os.Exit(1)
		}
		if resp.Status != clientv1.Status_STATUS_SUCCESS {
			fmt.Fprintf(os.Stderr, "Error [%s]: %s (leader hint: %s, retry after: %dms)\n",
				resp.Status.String(), resp.ErrorMessage, resp.LeaderHint, resp.RetryAfterMs)
			os.Exit(1)
		}
		if !resp.Found {
			fmt.Println("(not found)")
			os.Exit(0)
		}
		fmt.Println(string(resp.Value))

	case "set":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "Usage: raftkv-cli set <key> <value>")
			os.Exit(1)
		}
		key := args[1]
		val := args[2]
		reqID := *customReqID
		if reqID == "" {
			reqID = generateRequestID()
		}

		resp, err := client.Set(ctx, &clientv1.SetRequest{
			Key:       key,
			Value:     []byte(val),
			RequestId: reqID,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "RPC failed: %v\n", err)
			os.Exit(1)
		}
		if resp.Status != clientv1.Status_STATUS_SUCCESS {
			fmt.Fprintf(os.Stderr, "Error [%s]: %s (leader hint: %s, retry after: %dms)\n",
				resp.Status.String(), resp.ErrorMessage, resp.LeaderHint, resp.RetryAfterMs)
			os.Exit(1)
		}
		fmt.Println("OK")

	case "delete":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "Usage: raftkv-cli delete <key>")
			os.Exit(1)
		}
		key := args[1]
		reqID := *customReqID
		if reqID == "" {
			reqID = generateRequestID()
		}

		resp, err := client.Delete(ctx, &clientv1.DeleteRequest{
			Key:       key,
			RequestId: reqID,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "RPC failed: %v\n", err)
			os.Exit(1)
		}
		if resp.Status != clientv1.Status_STATUS_SUCCESS {
			fmt.Fprintf(os.Stderr, "Error [%s]: %s (leader hint: %s, retry after: %dms)\n",
				resp.Status.String(), resp.ErrorMessage, resp.LeaderHint, resp.RetryAfterMs)
			os.Exit(1)
		}
		fmt.Println("OK")

	case "status":
		resp, err := client.ClusterStatus(ctx, &clientv1.ClusterStatusRequest{})
		if err != nil {
			fmt.Fprintf(os.Stderr, "RPC failed: %v\n", err)
			os.Exit(1)
		}
		if resp.Status != clientv1.Status_STATUS_SUCCESS {
			fmt.Fprintf(os.Stderr, "Error [%s]: %s\n", resp.Status.String(), resp.ErrorMessage)
			os.Exit(1)
		}
		fmt.Printf("Leader ID: %s\n", resp.LeaderId)
		fmt.Printf("Term:      %d\n", resp.Term)
		fmt.Printf("Nodes (%d):\n", len(resp.Nodes))
		for _, n := range resp.Nodes {
			fmt.Printf("  - Node %s (Role: %s, LastContact: %d)\n", n.Id, n.Role, n.LastContact)
		}

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}
