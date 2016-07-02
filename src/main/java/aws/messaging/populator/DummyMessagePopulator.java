package aws.messaging.populator;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.util.Topics;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;

import java.util.GregorianCalendar;

public class DummyMessagePopulator {

    private static Boolean EMAIL_SUBSCRIBE = false;
    private static int NUM_MESSAGES = 50;

    public static void main(String[] args) throws InterruptedException {
        ClasspathPropertiesFileCredentialsProvider credentials = new ClasspathPropertiesFileCredentialsProvider();

        AmazonSNS sns = new AmazonSNSClient(credentials);
        sns.setRegion(Region.getRegion(Regions.US_EAST_1));
        AmazonSQS sqs = new AmazonSQSClient(credentials);
        sqs.setRegion(Region.getRegion(Regions.US_EAST_1));

        String topicArn = sns.createTopic(new CreateTopicRequest("testTopic")).getTopicArn();
        String queueUrl = sqs.createQueue(new CreateQueueRequest("testQueue")).getQueueUrl();

        Topics.subscribeQueue(sns, sqs, topicArn, queueUrl);

        if (EMAIL_SUBSCRIBE) {
            SubscribeRequest subRequest = new SubscribeRequest(topicArn, "email", "calvindn@gmail.com");
            sns.subscribe(subRequest);
            System.out.println("SubscribeRequest - " + sns.getCachedResponseMetadata(subRequest));
            System.out.println("Check your email and confirm subscription.");
        }

        for (int i = 0; i < NUM_MESSAGES; i++) {
            String msg = String.format("Test Message [%d] : %s", i, new GregorianCalendar().getTime());
            PublishRequest publishRequest = new PublishRequest(topicArn, msg);
            PublishResult publishResult = sns.publish(publishRequest);

            System.out.println(" ---");
            System.out.println("MessageId - " + publishResult.getMessageId());
            System.out.println("Message - " + publishRequest.getMessage());
        }
    }
}
