/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package master;

import java.io.BufferedReader;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.*;

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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 *
 * @author yolanda
 */
public class TwitterApp {

    private static final Logger logger = LoggerFactory.getLogger(TwitterApp.class);

    // Information necessary for accessing the Twitter API
    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;
    private String KafkaBrokerURL;
    private String filePath;
    private Properties props;

    // twitter stream to collect raw JSON data 
    private TwitterStream twitterStream;

    private void setContext(String[] args, int mode) {
        logger.debug("Setting parameters");
        props = new Properties();
        if (args.length < 5) {
            props.put("metadata.broker.list", "localhost:9092"); //Kafka Broker URLs     
        } else {
            props.put("metadata.broker.list", args[5]); //Kafka Broker URLs   
        }

        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        if (mode == 1) {
            if (args.length < 6) {
                filePath = "/home/yolanda/NetBeansProjects/twitterapp/twitterApp/tweetsLogFile.txt";
            } else {
                filePath = args[6];  //Filename          
            }
        } else if (args.length < 2) {
            consumerKey = "FjFVvfxNkx3aqv2X0KYJKnxIP";
            consumerSecret = "7IIB5CafrQIRlkHuO328KUQMgPlEjDtas3ciJYud7DdqTC0Kem";
            accessToken = "475871668-W6d8hmIVwpjaypzxSDEmjrufRP58pJU5pKJvmNaR";
            accessTokenSecret = "zFJUvJoOspsb39E2HcQsIQ6RfZBfYNuGGZhgQJ0gfJ9Df";
        } else {
            consumerKey = args[1];
            consumerSecret = args[2];
            accessToken = args[3];
            accessTokenSecret = args[4];
        }
    }

    private void readFromFile(Producer<String, String> producer) {
        logger.debug("reading from file: " + filePath);
        BufferedReader rd = null;
        try {
            try {
                rd = new BufferedReader(new FileReader(filePath));
                String line = null;
                logger.debug("Producing messages");

                while ((line = rd.readLine()) != null) {
                    producer.send(new KeyedMessage<String, String>("twitter-topic", line));
                }
                logger.debug("Done sending messages");
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
                    producer.close();
                }
            } catch (IOException ex) {
                logger.error("IO error while cleaning up", ex);
                logger.trace(null, ex);
            }
        }
    }

    private void readFromTwitterApi(final Producer<String, String> producer) {
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
        final Map<String, String> headers = new HashMap<>();

        /* Twitter listener */
        StatusListener listener;
        listener = new StatusListener() {
            @Override
            // The onStatus method is executed every time a new tweet comes in.
            public void onStatus(Status status) {
                logger.info(status.getUser().getScreenName() + ": " + status.getText());

                KeyedMessage<String, String> data = new KeyedMessage<>("twitter-topic", DataObjectFactory.getRawJSON(status));
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

    private void start(int mode) {
        ProducerConfig config = new ProducerConfig(props);
        final Producer<String, String> producer = new Producer<String, String>(config);

        if (mode == 1) { // Mode = 1, reads from file 
            readFromFile(producer);
        } else { // Mode = 2, reads from twitter API
            readFromTwitterApi(producer);

        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        System.out.println("Simple twitter App with param " + args[0]);
        int mode = 0;
        if (args.length > 0) {
            try {
                mode = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Argument" + args[0] + " must be an integer.");
                System.exit(1);
            }

            try {
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
