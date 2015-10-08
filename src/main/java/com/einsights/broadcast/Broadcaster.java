/*
 * Copyright (c) Einsights Pte. Ltd. - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.einsights.broadcast;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.SubscribeRequest;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/broadcast")
public class Broadcaster {
    
    private static String OK = "OK";

    @RequestMapping(value = "/send", method = RequestMethod.GET)
    public String send(@RequestParam("sender") final String sender,
            @RequestParam("messageType") final String messageType, @RequestParam("message") final String message) {
        // create a new SNS client and set endpoint
        AmazonSNSClient snsClient = new AmazonSNSClient(new ClasspathPropertiesFileCredentialsProvider());
        snsClient.setRegion(Region.getRegion(Regions.US_WEST_2));

        // publish to an SNS topic
        String topicArn = "arn:aws:sns:us-west-2:401612810481:" + messageType;
        PublishRequest publishRequest = new PublishRequest(topicArn, message);
        PublishResult publishResult = snsClient.publish(publishRequest);
        // print MessageId of message published to SNS topic
        return "MessageId - " + publishResult.getMessageId() + "<br/>" +
               OK;
    }

    @RequestMapping(value = "/addTopic", method = RequestMethod.GET)
    public String addTopic(@RequestParam("accountNo") final String accountNo,
            @RequestParam("messageType") final String messageType) {
        return "OK";
    }

    @RequestMapping(value = "/deleteTopic", method = RequestMethod.GET)
    public String deleteTopic(@RequestParam("accountNo") final String accountNo,
            @RequestParam("messageType") final String messageType) {
        return "OK";
    }

    @RequestMapping(value = "/addSubscription", method = RequestMethod.GET)
    public String addSubscription(@RequestParam("accountNo") final String accountNo,
            @RequestParam("messageType") final String messageType, @RequestParam("sender") final String userId,
            @RequestParam("channelType") final String channelType) {

        // create a new SNS client and set endpoint
        AmazonSNSClient snsClient = new AmazonSNSClient(new ClasspathPropertiesFileCredentialsProvider());
        snsClient.setRegion(Region.getRegion(Regions.US_WEST_2));

        // publish to an SNS topic
        String topicArn = "arn:aws:sns:us-west-2:401612810481:" + messageType;

        // subscribe to an SNS topic
        SubscribeRequest subRequest = new SubscribeRequest(topicArn, "email", "anhvurz90@gmail.com");
        snsClient.subscribe(subRequest);

        // get request id for SubscribeRequest from SNS metadata
        return "SubscribeRequest - " + snsClient.getCachedResponseMetadata(subRequest) + "<br/>" +
               "Check your email and confirm subscription." + "<br/>" +
               "OK";
    }

    @RequestMapping(value = "/deleteSubscription", method = RequestMethod.GET)
    public String deleteSubscription(@RequestParam("accountNo") final String accountNo,
            @RequestParam("messageType") final String messageType, @RequestParam("sender") final String userId,
            @RequestParam("channelType") final String channelType) {
        return "OK";
    }

}
