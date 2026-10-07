package main

import (
	"fmt"
	"os"
	"time"
	"strconv"
	"strings"
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

	ch, err := conn.Channel()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, userName),
		routing.ArmyMoveKey,
		pubsub.Transient,
		handlerMove(state, ch),
	)
	
	if err != nil {
		fmt.Printf("%v. Terminating the Client...\n", err)
		os.Exit(1)
	}
	
	handleWar(state, conn)	

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
				fmt.Printf("%s %s %s\n", "move", move.ToLocation, move.Units)	
				err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, userName), move)
				if err != nil {
					fmt.Printf("%v\n", err)
					continue
				}
				fmt.Printf("the move %s was published successfully\n", move.ToLocation)
				
			case "status":
				state.CommandStatus()
			case "help":
				gamelogic.PrintClientHelp()
			case "quit", "exit":
				fmt.Println("Exiting...\n")
				os.Exit(0)
			case "spam":
				arg := strings.Join(inputs[1:], "")
				n, err := strconv.Atoi(arg)
				if err != nil {
					fmt.Printf("Wrong argument %s for the spam command. Please provide an integer.", arg)
				}
				
				for i := 0; i < n; i ++ {
					msg := gamelogic.GetMaliciousLog()
					err := pubsub.PublishGob(
						ch, 	
						routing.ExchangePerilTopic,
						fmt.Sprintf("%s.%s", routing.GameLogSlug, state.Player.Username),
						routing.GameLog{
							Message: msg,
							Username: state.Player.Username,
							CurrentTime: time.Now(),
						},
					)
					if err != nil {
						fmt.Printf("Wrong argument %s for the spam command. Please provide an integer.", arg)
					}
				}
				
			default:
				fmt.Printf("Unknown command %s. Skipping...\n", inputs[0])
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState)pubsub.AckType{
	return func(r routing.PlayingState)pubsub.AckType{
		defer fmt.Print("> ")
		fmt.Println("consuming Pause message...")
		fmt.Println("Set Game Status to Paused.")
		gs.HandlePause(r)
		fmt.Println("Set Ack")
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove)pubsub.AckType{
	return func(m gamelogic.ArmyMove)pubsub.AckType{
		defer fmt.Print("> ")
		fmt.Println("\nconsuming Move message...")
		out := gs.HandleMove(m)
		switch out {
		case gamelogic.MoveOutcomeSafe:
			fmt.Println("Set Ack")
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			fmt.Println("Set NackDiscard")
			return pubsub.NackDiscard
		case gamelogic.MoveOutcomeMakeWar:
			w := gamelogic.RecognitionOfWar{
  				 	Attacker: m.Player,
				   	Defender: gs.GetPlayerSnap(),
				}
			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic, 
				fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.Player.Username),
				w)
			if err != nil {
				fmt.Printf("error: %v\n", err)
				fmt.Println("Set NAckRequeue")
				return pubsub.NackRequeue
			}

			fmt.Println("Set Ack")
			return pubsub.Ack
		default:
			fmt.Println("Set NackDiscard")
			return pubsub.NackDiscard
		}
	}
}


func handleWar(gs *gamelogic.GameState, conn *amqp.Connection){
	defer fmt.Print("> ")
	err := pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		"war",
		fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix),
		pubsub.Durable,
		func(msg gamelogic.RecognitionOfWar) pubsub.AckType{
			fmt.Println("Recognition of War!")
			ch, err := conn.Channel()
			if err != nil {
				return pubsub.NackRequeue
			}

			outcome, w, l := gs.HandleWar(msg)
			switch outcome {
			case gamelogic.WarOutcomeNotInvolved:
				return pubsub.NackRequeue
			case gamelogic.WarOutcomeNoUnits:
				return pubsub.NackDiscard
			case gamelogic.WarOutcomeOpponentWon,  gamelogic.WarOutcomeYouWon:
				return publishGameLog(ch, msg.Attacker.Username, fmt.Sprintf("%s won a war against %s", w, l))
			case gamelogic.WarOutcomeDraw:
				return publishGameLog(ch, msg.Attacker.Username, fmt.Sprintf("A was between %s and %s resulted in a draw", w, l))
			default:
				fmt.Printf("Unknown recognition of war '%s'", msg)
				return pubsub.NackDiscard
			}
		},
	)
	
	if err != nil {
		fmt.Printf("%v. Terminating the Client...\n", err)
		os.Exit(1)
	}
}

func publishGameLog(ch *amqp.Channel, usernameWhoStartedWar, logMessage string)pubsub.AckType{
	gameLog := routing.GameLog{
		Username: usernameWhoStartedWar,
		Message:logMessage,
		CurrentTime: time.Now(),	
	}
	err := pubsub.PublishGob(
	ch, 
	routing.ExchangePerilTopic,
	fmt.Sprintf("%s.%s", routing.GameLogSlug, usernameWhoStartedWar),
	gameLog)
	if err != nil {
		return pubsub.NackRequeue
	}
	
	return pubsub.Ack	
}
