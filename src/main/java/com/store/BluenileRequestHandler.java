package com.store;

import com.google.api.client.http.HttpMethods;
import com.google.cloud.ServiceOptions;
import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.gson.*;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.http.HttpStatus;


import java.io.BufferedWriter;
import java.io.IOException;


import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * This is a HTTP cloud function - which accepts http requests and provide a http response back.
 * This fucntion relays input payload to a pub/sub to allow any listener work on input asynchronously.
 */
public class BluenileRequestHandler implements HttpFunction {

    private final static String NUMBERS = "num";
    private final static String JSON_CONTENT_TYPE = "application/json";
    private static final Gson gson = new Gson();
    private static final Logger logger = Logger.getLogger(BluenileRequestHandler.class.getName());

    // Fucntion for Bluenile demo
    @Override
    public void service(HttpRequest request, HttpResponse response)
            throws IOException {
        BufferedWriter writer = response.getWriter();
        JsonObject requestBody = null;

        if (!request.getMethod().equals(HttpMethods.POST)) {
            response.setStatusCode(HttpStatus.SC_FORBIDDEN, "Bad Request");
            writer.write("403 Bad Request");
            logger.log(Level.SEVERE, "403 Bad Request");
            return;
        }

        // check if input is being passed through request body
        if (request.getReader() != null && request.getContentType().toString().contains(JSON_CONTENT_TYPE)) {
            requestBody = gson.fromJson(request.getReader(), JsonObject.class);
            logger.log(Level.INFO, "Request content type : " + request.getContentType());
        }

        // Return bad request if query params are not present in request
        if ((!request.getQueryParameters().containsKey(NUMBERS) ||
                request.getQueryParameters().get(NUMBERS) == null) &&
                !request.getContentType().toString().contains(JSON_CONTENT_TYPE)) {

            response.setStatusCode(HttpStatus.SC_BAD_REQUEST, "Bad Request");
            writer.write("Bad Request");
            logger.log(Level.SEVERE, "400 Bad Request");
            return;
        }
        try {
            List<String> numbers = request.getQueryParameters().get(NUMBERS);
            System.out.println("Queryparam String is" + numbers);

            // this is for input  coming as request body in POST Request
            if (requestBody != null && requestBody.get(NUMBERS) != null) {
                //send body to pub/sub
                publishToPubSub(requestBody);
            }

            // this is for input param coming as params in POST Request
            else if (numbers != null && numbers.size() > 0) {
                logger.log(Level.INFO, "200 Good Request");
                String inputPayload = numbers.get(0);
                List<Integer> convertedNumberList = Stream.of(inputPayload.split(","))
                        .map(String::trim)
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());
                JsonObject jsonObject = new JsonObject();
                JsonArray array = new JsonArray();
                for (Integer number : convertedNumberList) {
                    JsonElement element = new JsonPrimitive(number);
                    array.add(element);
                }
                jsonObject.add(NUMBERS, array);
                publishToPubSub(jsonObject);
            }

            response.setStatusCode(HttpStatus.SC_OK, "Good Request");
            writer.write("Good Request");
            logger.log(Level.INFO, "200 Good Request");

        } catch (NumberFormatException nfe) {
            response.setStatusCode(HttpStatus.SC_BAD_REQUEST, "Bad Input");
            writer.write("Bad Input");
            logger.log(Level.SEVERE, "400 Bad Request");
        } catch (Exception e) {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Server side error");
            writer.write("Server side error");
            logger.log(Level.SEVERE, "500 Server Error");
            e.printStackTrace();
        }
    }

    /**
     * Send payload to pub/sub using GCP cloud pub/sub package
     *
     * @param numberJsonObject - input received from Request
     */
    public void publishToPubSub(JsonObject numberJsonObject) {
        Publisher publisher = this.publisher;
        List<Integer> numbers = new ArrayList<>();
        try {
            JsonArray arrayValue = numberJsonObject.get(NUMBERS).getAsJsonArray();
            for (JsonElement element : arrayValue) {
                int number = element.getAsInt();
                numbers.add(number);
            }

            logger.info("payload is" + numbers);

            // creating pub sub payload pojo
            Payload payload = new Payload();
            payload.setNumbers(numbers);
            payload.setOrderId(new Random().nextInt());
            final String payloadMessage = gson.toJson(payload);
            logger.info("payloadMessage is" + payloadMessage);

            // Get Topic id from cloud function environment variable. Topic should be provisioned first.
            String topicId = System.getenv("PUBSUB_TOPIC");
            if (topicId == null) {
                topicId = "nile-store-numbers";  // This is to test in local, have this in environment variable in local
            }

            // create a publisher on the topic
            if (publisher == null) {
                ProjectTopicName topicName =
                        ProjectTopicName.newBuilder()
                                .setProject(ServiceOptions.getDefaultProjectId())
                                .setTopic(topicId)
                                .build();
                logger.log(Level.INFO, "topic name is -" + topicName);

                publisher = Publisher.newBuilder(topicName).build();

                PubsubMessage pubsubMessage =
                        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(payloadMessage)).build();
                logger.log(Level.INFO, "pub sub message is -" + pubsubMessage.toString());

                //publish payload to pub/sub
                publisher.publish(pubsubMessage);
                publisher.publishAllOutstanding();

            }

        } catch (IOException io) {
            logger.log(Level.SEVERE, "IO Exception");
            io.printStackTrace();
        }
    }

    private Publisher publisher;

    // Default constructor
    public BluenileRequestHandler() {
    }

    BluenileRequestHandler(Publisher publisher) {
        this.publisher = publisher;
    }
}

class Payload {

    public Payload() {
    }

    List<Integer> getNumbers() {
        return numbers;
    }

    public void setNumbers(List<Integer> numbers) {
        this.numbers = numbers;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    private List<Integer> numbers;

    private int orderId;

}



