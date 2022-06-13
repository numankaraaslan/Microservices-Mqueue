package com.aldimbilet.mailservice;

import java.util.Random;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.handler.annotation.Payload;
import com.aldimbilet.pojos.UserInfoPojo;
import lombok.extern.slf4j.Slf4j;

// Lombok directly injects an SLF4J logger inside this class
// This is lombok.extern.slf4j.Slf4j annotation, not usual SLF4J
// This way you can use log.info or log.error directly anywhere you want
@Slf4j
@SpringBootApplication
public class AldimbiletMailServicesApplication
{
	public static void main(String[] args)
	{
		SpringApplication.run(AldimbiletMailServicesApplication.class, args);
	}

	// If you forget this, the app will give "Listener method could not be invoked with the incoming message" error
	// And also you will get "Cannot convert from [[B] to [<your custom class>] for GenericMessage" error as root cause
	@Bean
	public Jackson2JsonMessageConverter producerJackson2MessageConverter()
	{
		return new Jackson2JsonMessageConverter();
	}

	// This method is a rabbitmq queue listener, you can't and won't listen exchanges or routing keys
	// It is listening for the messages in the queue named "emailReceiptQueue"
	// It is working with FIFO logic (first in first out)
	// Make sure you have a converter bean defined somewhere to receive custom classes
	// "emailReceiptQueue" is a topic exchange queue
	// And that topic receives messages with "email.receipt.*" format
	// It could be "email.receipt.special" or "email.receipt.normal" or ...etc
	// You don't have to specify @Payload annotation
	@RabbitListener(queues = "emailReceiptQueue")
	public void sendReceiptEmail(@Payload UserInfoPojo userInfo)
	{
		// Lombok gave us the logger and we can use it
		log.info("sending email receipt to '" + userInfo.getEmail() + "'");
		while (new Random().nextInt(99999999) != 55555555)
		{
			// Some imaginary operation that takes time
			// Purely for demonstration purposes
			// You would probably run a thread and wait for it to complete here
		}
		log.info("Handled the message and done with it");
		// RabbitMQ waits for acknowledgement to send the next message inside the queue
		// Acknowledgement means this method execution is done, even without any return value
		// That is why there is a weird while loop, it simulates this long running process
	}

	// This method is a rabbitmq queue listener, you can't and won't listen exchanges or routing keys
	// It is listening for the messages in the queue named "emailCancelationQueue"
	// It is working with FIFO logic (first in first out)
	// Make sure you have a converter bean defined somewhere to receive custom classes
	// "emailCancelationQueue" is a direct exchange queue
	// And that topic receives messages with exactly "email.cancelation" format, nothing else
	// You don't have to specify @Payload annotation
	@RabbitListener(queues = "emailCancelationQueue")
	public void sendCancelationEmail(@Payload UserInfoPojo userInfo) throws Exception
	{
		// Lombok gave us the logger and we can use it
		log.info("sending cancelation email to '" + userInfo.getEmail() + "'");
		while (new Random().nextInt(99999999) != 55555555)
		{
			// Some imaginary operation that takes time
			// Purely for demonstration purposes
			// You would probably run a thread and wait for it to complete here
		}
		if (new Random().nextInt(10) == 3)
		{
			// Randomly throw an error to imulate dead letters
			throw new Exception("bişeyler bişeyler");
		}
		log.info("Handled the message and done with it");
		// RabbitMQ waits for acknowledgement to send the next message inside the queue
		// Acknowledgement means this method execution is done, even without any return value
		// That is why there is a weird while loop, it simulates this long running process
	}
}
