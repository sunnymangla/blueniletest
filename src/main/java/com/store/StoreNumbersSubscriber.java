package com.store;

import com.google.cloud.datastore.*;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.gson.Gson;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is a cloud function whose accept() method gets called when a topic gets a message from producer & publishes
 * to all subscribers.
 * This function is event base trigger . Cloud function must be subscribed to a topic before it starts receiving event.
 * This function writes payload received in cloud data store number entity.
 */

public class StoreNumbersSubscriber implements BackgroundFunction<StoreNumbersSubscriber.PubSubMessage> {
    private static final Logger logger = Logger.getLogger(StoreNumbersSubscriber.class.getName());
    private final static String KIND = "numbers";
    private static final Gson gson = new Gson();

    @Override
    public void accept(PubSubMessage message, Context context) {
        String data = message.data != null
                ? new String(Base64.getDecoder().decode(message.data))
                : "";
        logger.info(data.isEmpty() ? "payload is empty" : data);

        try {
            if (!data.isEmpty()) {
                logger.info("invoke writing data to datastore");
                writeData(data);
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "500 server side error");
            e.printStackTrace();
        }
    }

    public static class PubSubMessage {
        String data;
        Map<String, String> attributes;
        String messageId;
        String publishTime;
    }

    public void writeData(String message) {
        Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
        NumbersPayload payload = gson.fromJson(message, NumbersPayload.class);
        int totalSum = 0;
        List<LongValue> values = new ArrayList<LongValue>();


        for (Integer number : payload.getNumbers()) {
            totalSum = totalSum + number;
            System.out.println("total sum is" + totalSum);
            values.add(LongValue.newBuilder(number).build());
        }


        // The name/ID for the new entity
        String name = "add-operation";
        // The Cloud Datastore key for the new entity
        Key taskKey = datastore.newKeyFactory().setKind(KIND).newKey(UUID.randomUUID().toString());
        System.out.println("doing DB operations");
        // Prepares the new entity
        Entity task = Entity.newBuilder(taskKey)
                .set("action", "add").set("result", totalSum).set("numbers", values)
                .build();

        // Saves the entity
        datastore.put(task);

        System.out.printf("Saved %s: %s%n", task.getKey(), task.getString("action"));

        //Retrieve entity
        Entity retrieved = datastore.get(taskKey);

        System.out.printf("Retrieved %s: %s%n", taskKey.getId(), retrieved.getString("action"));


    }

}

class NumbersPayload {

    public NumbersPayload() {
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