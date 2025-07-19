package mars;

import akka.actor.AbstractActor;
import akka.actor.Props;

import java.io.Serializable;

public class Client extends AbstractActor{
    public String name;
    public Logger logger;

    // MESSAGES --------------------------------------------
    public static class UpdateMsg implements Serializable {
        private final Integer key;
        private final String value;
        public UpdateMsg(Integer key, String value){
            this.key = key;
            this.value = value;
        }
    }

    public static class GetMsg implements Serializable{
        private final Integer key;
        public GetMsg(Integer key){
            this.key = key;
        }
    }

    // CONSTRUCTOR -----------------------------------------
    public Client(String name, Logger logger){
        this.name = name;
        this.logger = logger;
    }

    static public Props props(String name, Logger logger) {
        return Props.create(Client.class, () -> new Client(name, logger));
    }

    public void onUpdateMsg(){

    }

    public void onGetMsg(){

    }

    //define the mapping between the received message types and actor methods
    @Override
    public Receive createReceive() {
      return receiveBuilder()
        .matchAny(msg -> {
            //ignore all messages for now
            //TODO: write actor methods
            System.out.println("Message received " + msg);
            logger.log(this.name, "Message received");
        })
        .build();
    }
}