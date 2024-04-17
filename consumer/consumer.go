package main

import (
	"encoding/json"
	"log"
	"github.com/gofiber/fiber/v2"
)
type Comment struct{
	Text string `json:"text" form:"text"`
	
}
func main() {
	app := fiber.New()
	api := app.Group("/api")
	api.Post("/comments", createComment)
	app.Listen(":3000")
}

func createComment(c *fiber.Ctx) {
	cmnt := new(Comment)
	if err := c.BodyParser(cmnt); err != nil {
		log.Println(err)
		c.Status(500).JSON(&fiber.Map{
         "sucess":false,
			"message":err,
		})
		return err
	}
	cmntInbytes,err:=json.Marshal(cmnt)
	PushCommentToQueue("comments",cmntInbytes)
	c.JSON(&fiber.Map{
		"success":true,
		"message":"Comment created",
		"comment":cmnt,
	})
	if err!=nil{
		log.Println(err)
		c.Status(500).JSON(&fiber.Map{
			"success":false,
			"message":err,
		})
		return
	}
	
}
func ConnectProducer(brokersUrl []string) (*sarama.SyncProducer,error){
config:=sarama.NewConfig()
config.Producer.Return.success=true
config.Producer.RequiredAcks=sarama.waitForAll
config.Producer.Retry.Max=3
conn, err:=sarama.NewSyncProducer(brokersUrl,config)
if err!=nil{
	return nil, err
}
return conn, nil
}
func PushCommentToQueue(topic string, message []byte) error{
	brokersUrl:=[]string{"localhost:39092"}
	producer,err:=ConnectProducer(brokersUrl)
	if err!=nil{
		return err
	}
	defer producer.Close()
	msg:=&sarama.ProducerMessage{
		Topic:topic,
		Value:sarama.StringEncoder(message)
	}
	partition, offset, err:=producer.SendMessage(msg)
   if err!=nil{
	return err
   }
   fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
return nil

}