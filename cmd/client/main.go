package main

import (
	"fmt"
	"os"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)


const connectionString = "amqp://guest:guest@localhost:5672/" 
func main(){
	for {
		fmt.Println("Starting Peril client...")
		conn, err := amqp.Dial(connectionString)
		if err != nil {
			fmt.Printf("%v. Terminating the Client...", err)
			os.Exit(1)
		}

		userName, err := gamelogic.ClientWelcome()
		if err != nil {
			fmt.Printf("%v. Terminating the Client...", err)
			os.Exit(1)
		}
		
		_, _, err = pubsub.DeclareAndBind(
			conn,
			routing.ExchangePerilDirect,
			fmt.Sprintf("%s.%s", routing.PauseKey,userName),
			routing.PauseKey,
			pubsub.Transient,
		)
		
		if err != nil {
			fmt.Printf("%v. Terminating the Client...", err)
			os.Exit(1)
		}
	}
}
