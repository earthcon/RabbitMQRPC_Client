package com.landy.RabbitMQRPC;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class RPCClient implements AutoCloseable {

    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue";

    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public static void main(String[] argv) {
        try (RPCClient fibonacciRpc = new RPCClient()) {
            for (int i = 0; i < 3; i++) {
                String i_str = Integer.toString(i);
                System.out.println(" [x] Requesting fib(" + i_str + ")");
                String response = fibonacciRpc.call(i_str);
                System.out.println(" [.] Got '" + response + "'");
            }
            int index = 0;
            while(true) {
                String i_str =  Integer.toString(index);
                String response = fibonacciRpc.call2(i_str);
                if(response.equals("3")) {
                    System.out.println("3333333333333333333333333333");
                    break;
                }
                else if(response.equals("0")) {
                    System.out.println("0000000000000000000000000000000");
                }
                else if(response.equals("1")) {
                    System.out.println("1111111111111111111111111111111");
                }
                else if ( response.equals("1111")){
                    System.out.println("null");
                    System.out.println("null");
                }
            }

        } catch (NullPointerException | IOException | TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String call(String message) throws IOException, InterruptedException {
        final String corrId = message;//UUID.randomUUID().toString();

        String replyQueueName = "res_queue";//channel.queueDeclare().getQueue();

        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));


        return message;
    }


    public String call2(String message) throws IOException, InterruptedException {
        final BlockingQueue<String> response = new ArrayBlockingQueue<>(100);
        String replyQueueName = "res_queue";
        //channel.queueDeclare(replyQueueName, false, false, false, null);
        //channel.queuePurge(replyQueueName);

        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {

            System.out.println("message : " +  delivery.getProperties().getCorrelationId() + " : ===========================");
            System.out.println("getbody : " + (new String(delivery.getBody())) + " : ===========================");
            response.offer(new String(delivery.getBody(), "UTF-8"));

            //if (delivery.getProperties().getCorrelationId().equals(message)) {
            //    long nn = delivery.getProperties().getBodySize();
            //    response.offer(new String(delivery.getBody(), "UTF-8"));
            //}
        }, consumerTag -> {
        });

        String result = "1111";

        try {
            int count = 0;
            for( int i = 0 ; i < 3 ; i++) {
                if ( response.size() == 0) {
                    break;
                }
                result = response.poll(5000, TimeUnit.MICROSECONDS);

                System.out.println("result : " + result + " : ===========================");
            }

            channel.basicCancel(ctag);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return result;
    }

    public void close() throws IOException {
        connection.close();
    }
}
