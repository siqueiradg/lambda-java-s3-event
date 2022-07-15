import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.UUID;

public class Handler implements RequestHandler<S3EventNotification, Void> {

    private LambdaLogger logger;

    @Override
    public Void handleRequest(S3EventNotification s3Event, Context context) {
        this.logger = context.getLogger();

        AmazonS3 s3client = AmazonS3ClientBuilder
                .standard()
                .build();

        S3EventNotification.S3Entity s3 = s3Event.getRecords().get(0).getS3();
        S3Object object = s3client.getObject(new GetObjectRequest(s3.getBucket().getName(), s3.getObject().getKey()));

        try {
            logger.log("inicio processamento");
            processFile(object.getObjectContent());
            logger.log("fim processamento");
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    private void processFile(InputStream objectData) throws IOException {
        final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
        logger.log("inicio envio");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(objectData))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sendQueue(sqs, line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.log("fim envio");
        objectData.close();
    }

    private void sendQueue(AmazonSQS sqsConnection, String content) {
        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withMessageGroupId("teste")
                .withMessageDeduplicationId(UUID.randomUUID().toString())
                .withQueueUrl("fifo")
                .withMessageBody(content);
        sqsConnection.sendMessage(send_msg_request);
    }

}
