package aws.messaging.subscriber;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.util.Topics;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import java.util.List;

public class Subscriber {

    public static void main(String[] args) throws InterruptedException {
        ClasspathPropertiesFileCredentialsProvider credentials = new ClasspathPropertiesFileCredentialsProvider();

        AmazonSNS sns = new AmazonSNSClient(credentials);
        sns.setRegion(Region.getRegion(Regions.US_EAST_1));
        AmazonSQS sqs = new AmazonSQSClient(credentials);
        sqs.setRegion(Region.getRegion(Regions.US_EAST_1));

        String topicArn = sns.createTopic(new CreateTopicRequest("testTopic")).getTopicArn();
        String queueUrl = sqs.createQueue(new CreateQueueRequest("testQueue")).getQueueUrl();

        Topics.subscribeQueue(sns, sqs, topicArn, queueUrl);

        while (true) {
            List<Message> messages = sqs.receiveMessage(new ReceiveMessageRequest(queueUrl)).getMessages();
            messages.forEach((message) -> {
                System.out.println("Processing Message: " + message);

                // delete a message
                String messageReceiptHandle = messages.get(0).getReceiptHandle();
                sqs.deleteMessage(new DeleteMessageRequest()
                        .withQueueUrl(queueUrl)
                        .withReceiptHandle(messageReceiptHandle));
            });
        }


    }
}
