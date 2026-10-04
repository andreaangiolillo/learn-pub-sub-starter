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
	fmt.Println("Starting Peril client...")
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Printf("%v. Terminating the Client...\n", err)
		os.Exit(1)
	}

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("%v. Terminating the Client...\n", err)
		os.Exit(1)
	}
	
	state := gamelogic.NewGameState(userName)
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, userName),
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(state),
	)
	
	if err != nil {
		fmt.Printf("%v. Terminating the Client...\n", err)
		os.Exit(1)
	}

	for {
		inputs := gamelogic.GetInput()
		if len(inputs) == 0 {
			continue
		}
		
		switch inputs[0] {
			case "spawn":
				err := state.CommandSpawn(inputs)
				if err != nil {
					fmt.Printf("%v. Terminating the Client...\n", err)
					os.Exit(1)
				}
			case "move":
				move, err := state.CommandMove(inputs)
				if err != nil {
					fmt.Printf("%v\n", err)
					continue
				}
				fmt.Printf("%s %s 1", "move", move.ToLocation)
				
			case "status":
				state.CommandStatus()
			case "help":
				gamelogic.PrintClientHelp()
			case "quit", "exit":
				fmt.Println("Exiting...\n")
				os.Exit(0)
			case "spam":
				fmt.Println("Spamming not allowed yet!\n")
			default:
				fmt.Printf("Unknown command %s. Skipping...\n", inputs[0])
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState){
	return func(r routing.PlayingState){
		defer fmt.Print("> ")
		fmt.Println("consuming Pause message...")
		fmt.Println("Set Game Status to Paused.")

		gs.HandlePause(r)
	}
}
