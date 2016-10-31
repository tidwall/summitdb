package main

import (
  "flag"
  "fmt"

  "github.com/garyburd/redigo/redis"
)

func Command(conn redis.Conn, name string, args ...string) {
  reply, err := conn.Do(name, args[0], args[1])
  if err != nil {
    fmt.Printf("Error %v\n", err)
  }
  switch t := reply.(type) {
  case []byte:
    fmt.Println(string(t))
  case string:
    fmt.Println(t)
  default:
    fmt.Printf("Result %v\n", reply)
  }
}

func main() {
  var host = flag.String("h", "localhost" , "Server host")
  var port = flag.String("p", ":7481" , "Server port")
  flag.Parse()
  var url = *host + ":" + *port
  conn, err := redis.Dial("tcp", url)
  if err != nil {
    fmt.Println("Could not connect to SummitDB at "+url)
    fmt.Println("Please pass the right parameters")
    flag.PrintDefaults()
    return
  }
  fmt.Println("Connected to SummitDB at "+url)
  defer conn.Close()
  // Command(conn, "JSET", "user24", "age", "44")
  Command(conn, "JGET", "user24", "age")
}
