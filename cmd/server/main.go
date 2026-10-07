package main

import (
	"fmt"
	"os"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
)

const connectionString = "amqp://guest:guest@localhost:5672/" 
func main() {
	fmt.Println("Starting Peril server...")
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Printf("%v. Terminating the Server...", err)
		os.Exit(1)
	}
	
	defer conn.Close()

	fmt.Println("Connetion Exablished")
	ch, _, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		"game_logs",
		fmt.Sprintf("%s.*", routing.GameLogSlug),
		pubsub.Durable,
	)

	if err != nil {
		fmt.Printf("%v. Terminating the Server...", err)
		os.Exit(1)
	}

	pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		"game_logs",
		fmt.Sprintf("%s.*", routing.GameLogSlug),
		pubsub.Durable,
		handleGameLogs(),	
	)
	
	gamelogic.PrintServerHelp()

	for {
		inputs := gamelogic.GetInput()
		if len(inputs) == 0 {
			continue
		}

		for _, i := range inputs{
			switch i {
				case "pause":
					fmt.Println("Sending a pause message..")
				  	msg := routing.PlayingState{
						IsPaused: true,
					}
		 
					err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, msg)

					if err != nil {
						fmt.Printf("%v. Terminating the Server...", err)
						os.Exit(1)
			 		 }
				case "resume":
					fmt.Println("Sending a resume message..")
					msg := routing.PlayingState{
						IsPaused: false,
					}

				  	err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, msg)

					if err != nil {
						fmt.Printf("%v. Terminating the Server...", err)
						os.Exit(1)
			 		}
				case "quit", "exit":
					fmt.Println("Exiting...")
					os.Exit(0)
				case "help":
					gamelogic.PrintServerHelp()
				default:
					fmt.Printf("Unknown command %s, skipping...", i ) 
				}
			}
		}
	}


func handleGameLogs()func(routing.GameLog) pubsub.AckType{
	return func (gamelog routing.GameLog) pubsub.AckType {
		defer fmt.Print("< ")
		err := gamelogic.WriteLog(gamelog)
		if err != nil {
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}
