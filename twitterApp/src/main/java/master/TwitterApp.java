package master;

import java.io.BufferedReader;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.log4j.BasicConfigurator;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author Yolanda de la Hoz Simon - 53826071E
 * @version 1.2 12 de Diciembre de 2015
 */
public class TwitterApp {

    private static final Logger logger = LoggerFactory.getLogger(TwitterApp.class);

    private static final String KAFKA_TOPIC = "twitter-topic";
    // Information necessary for accessing the Twitter API
    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;
    private String filePath;
    private Properties props;

    // twitter stream to collect raw JSON data 
    private TwitterStream twitterStream;

     /**
     * Method to set the necessary parameters
     * @param args Arguments passed by command line       
     */
    private void setContext(String[] args, int mode) {
        logger.debug("Setting parameters");
        props = new Properties();
              
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        if (mode == 1) { // Reads from file
            if (args.length>5){
              props.put("metadata.broker.list", args[5]); //Kafka Broker URLs 
              filePath = args[6]; // Filepath is provided as a sixth argument.
            }else{
             props.put("metadata.broker.list", args[1]); //Kafka Broker URLs 
             filePath = args[2]; // Filepath is provided as a second argument. 
            }
          props.put("producer.type", "async");
        } else if (args.length < 2) {
            consumerKey = "FjFVvfxNkx3aqv2X0KYJKnxIP";
            consumerSecret = "7IIB5CafrQIRlkHuO328KUQMgPlEjDtas3ciJYud7DdqTC0Kem";
            accessToken = "475871668-W6d8hmIVwpjaypzxSDEmjrufRP58pJU5pKJvmNaR";
            accessTokenSecret = "zFJUvJoOspsb39E2HcQsIQ6RfZBfYNuGGZhgQJ0gfJ9Df";
            props.put("metadata.broker.list", args[5]); //Kafka Broker URLs 
        } else {
            consumerKey = args[1];
            consumerSecret = args[2];
            accessToken = args[3];
            accessTokenSecret = args[4];
            props.put("metadata.broker.list", args[5]); //Kafka Broker URLs 
        }
    }
 
     /**
     * Method to read tweets for a preloaded log file in JSON format  and send tweets from kafka topic
     * @param producer Kafka producer to send by the topic twitter-topic     
     */
    private void readFromFile(final ProducerConfig config){
    	final Producer<Integer, String> producer = new Producer<>(config);
        logger.debug("Reading from file: " + filePath);
        BufferedReader rd = null;
        try {
            try {
                rd = new BufferedReader(new FileReader(filePath));
                String line = null;

                while ((line = rd.readLine()) != null) {                
                    producer.send(new KeyedMessage<Integer, String>(
                    		KAFKA_TOPIC, line));
                   // logger.debug("----------------------------->Producing messages: " +line);
                }
               // logger.debug("Done sending messages");
            } catch (IOException ex) {
                logger.error("IO Error while producing messages", ex);
                logger.trace(null, ex);
            }
        } catch (Exception ex) {
            logger.error("Error while producing messages", ex);
            logger.trace(null, ex);
        } finally {
            try {
                if (rd != null) {
                    rd.close();
                }
                if (producer != null) {
                    logger.debug("Producer close");
                    producer.close();
                }
            } catch (IOException ex) {
                logger.error("IO error while cleaning up", ex);
                logger.trace(null, ex);
            }
        }
    }

    /**
    * Method to read tweets from the twitter API and send tweets from kafka topic
    * @param producer Kafka producer to send by the topic twitter-topic     
    */
    private void readFromTwitterApi(final ProducerConfig config) {
        final Producer<String, String> producer = new Producer<>(config);
        logger.debug("reading from twitter api");
        // Set up twitter properties 
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey(consumerKey);
        cb.setOAuthConsumerSecret(consumerSecret);
        cb.setOAuthAccessToken(accessToken);
        cb.setOAuthAccessTokenSecret(accessTokenSecret);
        cb.setJSONStoreEnabled(true);
        cb.setIncludeEntitiesEnabled(true);

        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        /* Twitter listener */
        StatusListener listener;
        listener = new StatusListener() {
            @Override
            // The onStatus method is executed every time a new tweet comes in.
            public void onStatus(Status status) {
                //logger.info(status.getUser().getScreenName() + ": " + status.getText());

                KeyedMessage<String, String> data = new KeyedMessage<>(KAFKA_TOPIC, DataObjectFactory.getRawJSON(status));
                producer.send(data);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
            }

            @Override
            public void onException(Exception ex) {
                logger.info("Shutting down Twitter sample stream...");
                twitterStream.shutdown();
            }

            @Override
            public void onStallWarning(StallWarning warning) {
            }
        };

        // Bind the listener 
        twitterStream.addListener(listener);

        twitterStream.sample();
    }

    /**
    * Method to start the twitter app with the selected mode
    * @param mode Mode to start the app. Mode 1 reads from file. Mode 2 reads from twitter API.     
    */
    private void start(int mode) {
        ProducerConfig config = new ProducerConfig(props);

        if (mode == 1) { // Mode = 1, reads from file 
            readFromFile(config);
        } else { // Mode = 2, reads from twitter API
            readFromTwitterApi(config);

        }
    }

    /**
    * Main method
    * @param args Arguments: mode, apiKey, apiSecret, tokenValue, tokenSecret, kafkaBrokerURL and filename    
    * @throws java.lang.Exception    
    */
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int mode = 0;
        if (args.length > 0) {
            System.out.println("Started twitter kafka producer with mode: " + args[0]);
            try {
                mode = Integer.parseInt(args[0]);
                if (mode==1 && args.length <2 ) {
                   System.out.println("To start the App with mode 1 it is required the filepath");
                   System.exit(1);  
                }
                if (mode==1 && args.length <3 ) {
                   System.out.println("To start the App with mode 1 it is required the kafka broker");
                   System.exit(1);  
                }
                if (mode==2 && args.length <5 ) {
                   System.out.println("To start the App with mode 2 it is required the apiKey, apiSecret, tokenValue, tokenSecret and kafkaBrokerURL");
                   System.exit(1);  
                }         

                TwitterApp app = new TwitterApp();
                app.setContext(args, mode);
                app.start(mode);

            } catch (Exception e) {
                logger.info(e.getMessage());
            }
            
        } else {
            System.out.println("Arguments: mode, apiKey, apiSecret, tokenValue, tokenSecret, kafkaBrokerURL and filename");
            System.exit(1);
        }

    }

}
