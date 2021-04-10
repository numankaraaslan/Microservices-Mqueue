package com.aldimbilet.website.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

@Component
// Bean names in this config is necessary to avoid confusion
// We are creating same bean types across spring boot
// If you don't specify bean names, spring boot will inject wrong
public class RabbitConfig
{
	@Bean(name = "emailReceiptTopic")
	public TopicExchange rabbitTopic()
	{
		return new TopicExchange("emailReceiptTopic");
	}

	@Bean(name = "emailReceiptQueue")
	public Queue emailReceiptQueue()
	{
		return new Queue("emailReceiptQueue", true);
	}

	@Bean
	@DependsOn(value =
	{ "emailReceiptTopic", "emailReceiptQueue" })
	public Binding emailReceiptBinding(TopicExchange exchange, Queue emailReceiptQueue)
	{
		// Bind the emailReceiptQueue to an exchange with a routing key definiton
		// The messages starting with "email.receipt." will be used in exchange to bind it to emailReceiptQueue
		return BindingBuilder.bind(emailReceiptQueue).to(exchange).with("email.receipt.*");
	}

	@Bean(name = "emailCancelationDirect")
	public DirectExchange rabbitDirect()
	{
		return new DirectExchange("emailCancelationDirect");
	}

	@Bean(name = "emailCancelationQueue")
	public Queue emailCancelationQueue()
	{
		// This one will redirect the undeliverable messages to deadLetterExchange with "email.cancelation.deadletter" route
		// Deadletter works as direct exchange (could have been topic or fanout too)
		return QueueBuilder.durable("emailCancelationQueue").withArgument("x-dead-letter-exchange", "deadLetterExchange").withArgument("x-dead-letter-routing-key", "email.cancelation.deadletter").build();
	}

	@Bean
	@DependsOn(value =
	{ "emailCancelationDirect", "emailCancelationQueue" })
	public Binding deadLetterBinding(@Value("emailCancelationDirect") DirectExchange direct, @Value("emailCancelationQueue") Queue emailCancelationQueue)
	{
		// Bind the emailCancelationQueue to an exchange with a routing key
		// The exact messages "email.cancelation" will be used in exchange to bind it to emailCancelationQueue
		return BindingBuilder.bind(emailCancelationQueue).to(direct).with("email.cancelation");
	}

	@Bean(name = "deadLetterExchange")
	public DirectExchange deadLetterExchange()
	{
		return new DirectExchange("deadLetterExchange");
	}

	@Bean(name = "deadLetterQueue")
	public Queue deadLetterQueue()
	{
		return QueueBuilder.durable("deadLetterQueue").build();
	}

	@Bean
	@DependsOn(value =
	{ "deadLetterExchange", "deadLetterQueue" })
	public Binding emailCancelationBinding(@Value("deadLetterExchange") DirectExchange deadExchange, @Value("deadLetterQueue") Queue deadLetterQueue)
	{
		// DO NO FORGET "spring.rabbitmq.listener.simple.default-requeue-rejected=false" in the application.properties of the receiver service !!
		// Bind the deadLetterQueue to the deadLetterExchange with a routing key
		// The exact messages "email.cancelation.deadletter" will be used in deadLetterExchange to bind it to deadLetterQueue
		return BindingBuilder.bind(deadLetterQueue).to(deadExchange).with("email.cancelation.deadletter");
	}

	@Bean
	private RabbitTemplate setReturnCallback(ConnectionFactory connectionFactory)
	{
		RabbitTemplate myCustomTemplate = new RabbitTemplate(connectionFactory);
		ConfirmCallback confirmCallback = new ConfirmCallback()
		{
			@Override
			public void confirm(CorrelationData correlationData, boolean ack, String cause)
			{
				// This log is to be notified if the payload is succesfully delivered to rabbitmq
				// It acks true even if the message goes to deadletter queue
				// It doesn't return any data because we are not expecting a message from the consumer
				System.err.println("Returned: " + correlationData.getReturned());
				System.err.println("Ack: " + ack);
				System.err.println("Cause: " + cause);
				System.err.println();
			}
		};
		myCustomTemplate.setConfirmCallback(confirmCallback);
		// If you forget this message converter, you can't send custom classes
		// Your app will give "SimpleMessageConverter only supports String, byte[] and Serializable payloads" error
		// If this bean is not initialized before the template, the same error occurs
		// The default converter is org.springframework.messaging.converter.SimpleMessageConverter
		myCustomTemplate.setMessageConverter(new Jackson2JsonMessageConverter());
		return myCustomTemplate;
	}
}
