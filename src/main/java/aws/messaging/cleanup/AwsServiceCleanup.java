package aws.messaging.cleanup;

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

public class AwsServiceCleanup {

    public static void main(String[] args) throws InterruptedException {
        ClasspathPropertiesFileCredentialsProvider credentials = new ClasspathPropertiesFileCredentialsProvider();

        AmazonSNS sns = new AmazonSNSClient(credentials);
        sns.setRegion(Region.getRegion(Regions.US_EAST_1));
        AmazonSQS sqs = new AmazonSQSClient(credentials);
        sqs.setRegion(Region.getRegion(Regions.US_EAST_1));

        String topicArn = sns.createTopic(new CreateTopicRequest("testTopic")).getTopicArn();
        String queueUrl = sqs.createQueue(new CreateQueueRequest("testQueue")).getQueueUrl();

        String subscriptionArn = Topics.subscribeQueue(sns, sqs, topicArn, queueUrl);

        sns.unsubscribe(subscriptionArn);
        sqs.deleteQueue(queueUrl);
        sns.deleteTopic(topicArn);
    }
}
