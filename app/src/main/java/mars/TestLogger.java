package mars;

import java.util.Random;

public class TestLogger {
    private Logger logger;
    private Random random = new Random();
    private String[] actors = {"CLIENT1", "NODE1", "MAIN"};

    public TestLogger(String path){
        logger = new Logger(path);
    }

    public void runTest(){
        for(int i=0; i<20; i++){
            int actorIndex = random.nextInt(actors.length);
            String actor = actors[actorIndex];
            String message = "Message #" + i;
            logger.log(actor, message);

            //sleep for 200ms to 500ms
            int delayMillis = 200 + random.nextInt(301);
            try {
                Thread.sleep(delayMillis);
            } catch (Exception e) {
                continue;
            }
        }

        logger.closeStream();
    }
}