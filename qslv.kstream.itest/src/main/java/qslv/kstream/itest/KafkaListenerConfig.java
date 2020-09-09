package qslv.kstream.itest;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties.AckMode;

import qslv.common.kafka.JacksonAvroDeserializer;
import qslv.common.kafka.ResponseMessage;
import qslv.common.kafka.TraceableMessage;
import qslv.data.BalanceLog;
import qslv.kstream.LoggedTransaction;
import qslv.kstream.PostingResponse;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.kstream.PostingRequest;

@Configuration
@EnableKafka
public class KafkaListenerConfig {
	private static final Logger log = LoggerFactory.getLogger(KafkaListenerConfig.class);

	@Autowired
	ConfigProperties configProperties;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Bean
	public Map<String,Object> listenerConfig() throws Exception {
		Properties kafkaconfig = new Properties();
		try {
			kafkaconfig.load(new FileInputStream(configProperties.getKafkaConsumerPropertiesPath()));
		} catch (Exception fileEx) {
			try {
				kafkaconfig.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(configProperties.getKafkaConsumerPropertiesPath()));
			} catch (Exception resourceEx) {
				log.error("{} not found.", configProperties.getKafkaConsumerPropertiesPath());
				log.error("File Exception. {}", fileEx.toString());
				log.error("Resource Exception. {}", resourceEx.toString());
				throw resourceEx;
			}
		}
		return new HashMap(kafkaconfig);
	}
	
	@Bean
	ArrayBlockingQueue<ResponseMessage<PostingRequest,PostingResponse>> responseExchangeQueue() {
		return new ArrayBlockingQueue<>(1000);
	}
	@Bean
	ArrayBlockingQueue<TraceableMessage<WorkflowMessage>> reservationMatchExchangeQueue() {
		return new ArrayBlockingQueue<>(1000);
	}
	@Bean
	ArrayBlockingQueue<TraceableMessage<WorkflowMessage>> transactionProcessorExchangeQueue() {
		return new ArrayBlockingQueue<>(1000);
	}
	@Bean
	ArrayBlockingQueue<LoggedTransaction> reservationByUuidExchangeQueue() {
		return new ArrayBlockingQueue<>(1000);
	}
	@Bean
	ArrayBlockingQueue<TraceableMessage<LoggedTransaction>> loggedTransactionExchangeQueue() {
		return new ArrayBlockingQueue<>(1000);
	}
	@Bean
	ArrayBlockingQueue<BalanceLog> balanceLogExchangeQueue() {
		return new ArrayBlockingQueue<>(1000);
	}
	
	/*
	configProperties.getEnhancedRequestTopic();
	configProperties.getResponseTopic();
	configProperties.getMatchReservationTopic();
	configProperties.getReservationByUuidTopic();
	configProperties.getLoggedTransactionTopic();
	configProperties.getBalanceLogStateStoreTopic();
	 */


    //----------------------------------------------------
	//--Response Message Consumer
    @Bean
    public ConsumerFactory<String, TraceableMessage<ResponseMessage<PostingRequest,PostingResponse>>> responseConsumerFactory() throws Exception {
    	
    	JacksonAvroDeserializer<TraceableMessage<ResponseMessage<PostingRequest,PostingResponse>>> jad
    				= new JacksonAvroDeserializer<>();
    	jad.configure(listenerConfig());
    	
        return new DefaultKafkaConsumerFactory<String, TraceableMessage<ResponseMessage<PostingRequest,PostingResponse>>>
        	(listenerConfig(), new StringDeserializer(),  jad);
    }
    
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, TraceableMessage<ResponseMessage<PostingRequest,PostingResponse>>>> 
    responseListenerContainerFactory(
    			ConsumerFactory<String, TraceableMessage<ResponseMessage<PostingRequest,PostingResponse>>> responseConsumerFactory) throws Exception {
    
        ConcurrentKafkaListenerContainerFactory<String, TraceableMessage<ResponseMessage<PostingRequest,PostingResponse>>>
        	factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(responseConsumerFactory);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
    
    //----------------------------------------------------
	//--Workflow Message Consumer
    @Bean
    public ConsumerFactory<String, TraceableMessage<WorkflowMessage>> workflowConsumerFactory() throws Exception {
    	
    	JacksonAvroDeserializer<TraceableMessage<WorkflowMessage>> jad = new JacksonAvroDeserializer<>();
    	jad.configure(listenerConfig());
    	
        return new DefaultKafkaConsumerFactory<String, TraceableMessage<WorkflowMessage>>
        	(listenerConfig(), new StringDeserializer(),  jad);
    }
    
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, TraceableMessage<WorkflowMessage>>> 
    workflowListenerContainerFactory(
    			ConsumerFactory<String, TraceableMessage<WorkflowMessage>> workflowConsumerFactory) throws Exception {
    
        ConcurrentKafkaListenerContainerFactory<String, TraceableMessage<WorkflowMessage>> 
        	factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(workflowConsumerFactory);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
    
    //----------------------------------------------------
	//--Reservation By UUID Message Consumer
    @Bean
    public ConsumerFactory<String, LoggedTransaction> reservationByUuidConsumerFactory() throws Exception {
    	
    	JacksonAvroDeserializer<LoggedTransaction> jad = new JacksonAvroDeserializer<>();
    	jad.configure(listenerConfig());
    	
        return new DefaultKafkaConsumerFactory<String, LoggedTransaction>
        	(listenerConfig(), new StringDeserializer(),  jad);
    }
    
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, LoggedTransaction>> 
    reservationByUuidListenerContainerFactory(
    			ConsumerFactory<String, LoggedTransaction> reservationByUuidConsumerFactory) throws Exception {
    
        ConcurrentKafkaListenerContainerFactory<String, LoggedTransaction> 
        	factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(reservationByUuidConsumerFactory);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
    
    //----------------------------------------------------
	//--Logged Transaction Message Consumer
    @Bean
    public ConsumerFactory<String, TraceableMessage<LoggedTransaction>> loggedTransactionConsumerFactory() throws Exception {
    	
    	JacksonAvroDeserializer<TraceableMessage<LoggedTransaction>> jad = new JacksonAvroDeserializer<>();
    	jad.configure(listenerConfig());
    	
        return new DefaultKafkaConsumerFactory<String, TraceableMessage<LoggedTransaction>>
        	(listenerConfig(), new StringDeserializer(),  jad);
    }
    
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, TraceableMessage<LoggedTransaction>>> 
    loggedTransactionListenerContainerFactory(
    			ConsumerFactory<String, TraceableMessage<LoggedTransaction>> loggedTransactionConsumerFactory) throws Exception {
    
        ConcurrentKafkaListenerContainerFactory<String, TraceableMessage<LoggedTransaction>> 
        	factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(loggedTransactionConsumerFactory);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
 
    //----------------------------------------------------
	//--Balance Log Message Consumer
    @Bean
    public ConsumerFactory<String, BalanceLog> balanceLogConsumerFactory() throws Exception {
    	
    	JacksonAvroDeserializer<BalanceLog> jad = new JacksonAvroDeserializer<>();
    	jad.configure(listenerConfig());
    	
        return new DefaultKafkaConsumerFactory<String, BalanceLog>
        	(listenerConfig(), new StringDeserializer(),  jad);
    }
    
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, BalanceLog>> 
    balanceLogListenerContainerFactory(
    			ConsumerFactory<String, BalanceLog> balanceLogConsumerFactory) throws Exception {
    
        ConcurrentKafkaListenerContainerFactory<String, BalanceLog> 
        	factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(balanceLogConsumerFactory);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

}
