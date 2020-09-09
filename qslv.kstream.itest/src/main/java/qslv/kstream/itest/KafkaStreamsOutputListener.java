package qslv.kstream.itest;

import java.util.concurrent.ArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import qslv.common.kafka.ResponseMessage;
import qslv.common.kafka.TraceableMessage;
import qslv.data.BalanceLog;
import qslv.kstream.LoggedTransaction;
import qslv.kstream.PostingResponse;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.kstream.PostingRequest;

@Component
public class KafkaStreamsOutputListener {
	private static final Logger log = LoggerFactory.getLogger(KafkaStreamsOutputListener.class);

	/*
		configProperties.getEnhancedRequestTopic();
		configProperties.getResponseTopic();
		configProperties.getMatchReservationTopic();
		configProperties.getReservationByUuidTopic();
		configProperties.getLoggedTransactionTopic();
		configProperties.getBalanceLogStateStoreTopic();
	*/
	@Autowired ArrayBlockingQueue<ResponseMessage<PostingRequest,PostingResponse>> responseExchangeQueue;
	@Autowired ArrayBlockingQueue<TraceableMessage<WorkflowMessage>> reservationMatchExchangeQueue;
	@Autowired ArrayBlockingQueue<TraceableMessage<WorkflowMessage>> transactionProcessorExchangeQueue;
	@Autowired ArrayBlockingQueue<LoggedTransaction> reservationByUuidExchangeQueue;
	@Autowired ArrayBlockingQueue<TraceableMessage<LoggedTransaction>> loggedTransactionExchangeQueue;
	@Autowired ArrayBlockingQueue<BalanceLog> balanceLogExchangeQueue;

	@KafkaListener(containerFactory = "workflowListenerContainerFactory", 
			topics = { "#{ @configProperties.enhancedRequestTopic }" }, groupId="kstream.itest")
	public void transactionProcessorListen(@Payload TraceableMessage<WorkflowMessage> message, Acknowledgment acknowledgment) {
		log.debug("transactionProcessorListen ENTRY");
		try {
			transactionProcessorExchangeQueue.put(message);
		} catch (InterruptedException e) {
			log.debug(e.getLocalizedMessage());
		}
		acknowledgment.acknowledge();
		log.debug("transactionProcessorListen EXIT");
	}
	
	@KafkaListener(containerFactory = "workflowListenerContainerFactory", 
			topics = { "#{ @configProperties.matchReservationTopic }" }, groupId="kstream.itest")
	public void matchReservationListen(@Payload TraceableMessage<WorkflowMessage> message, Acknowledgment acknowledgment) {
		log.debug("matchReservationListen ENTRY");
		try {
			reservationMatchExchangeQueue.put(message);
		} catch (InterruptedException e) {
			log.debug(e.getLocalizedMessage());
		}
		acknowledgment.acknowledge();

		log.debug("matchReservationListen EXIT");
	}
	
	@KafkaListener(containerFactory = "responseListenerContainerFactory", 
			topics = { "#{ @configProperties.responseTopic }" }, groupId="kstream.itest")
	public void responseListen(@Payload ResponseMessage<PostingRequest,PostingResponse> message, Acknowledgment acknowledgment) {
		log.debug("responseListen ENTRY");
		try {
			responseExchangeQueue.put(message);
		} catch (InterruptedException e) {
			log.debug(e.getLocalizedMessage());
		}
		acknowledgment.acknowledge();
		log.debug("responseListen EXIT");
	}

	@KafkaListener(containerFactory = "reservationByUuidListenerContainerFactory", 
			topics = { "#{ @configProperties.reservationByUuidTopic }" }, groupId="kstream.itest")
	public void reservationByUuidListen(@Payload LoggedTransaction message, Acknowledgment acknowledgment) {
		log.debug("reservationByUuidListen ENTRY");
		try {
			reservationByUuidExchangeQueue.put(message);
		} catch (InterruptedException e) {
			log.debug(e.getLocalizedMessage());
		}
		acknowledgment.acknowledge();
		log.debug("reservationByUuidListen EXIT");
	}

	@KafkaListener(containerFactory = "loggedTransactionListenerContainerFactory", 
			topics = { "#{ @configProperties.loggedTransactionTopic }" }, groupId="kstream.itest")
	public void loggedTransactionListen(@Payload TraceableMessage<LoggedTransaction> message, Acknowledgment acknowledgment) {
		log.debug("loggedTransactionListen ENTRY");
		try {
			loggedTransactionExchangeQueue.put(message);
		} catch (InterruptedException e) {
			log.debug(e.getLocalizedMessage());
		}
		acknowledgment.acknowledge();
		log.debug("loggedTransactionListen EXIT");
	}
	
	@KafkaListener(containerFactory = "balanceLogListenerContainerFactory", 
			topics = { "#{ @configProperties.balanceLogStateStoreTopic }" }, groupId="kstream.itest")
	public void balanceLogListen(@Payload BalanceLog message, Acknowledgment acknowledgment) {
		log.debug("balanceLogListen ENTRY");
		try {
			balanceLogExchangeQueue.put(message);
		} catch (InterruptedException e) {
			log.debug(e.getLocalizedMessage());
		}
		acknowledgment.acknowledge();
		log.debug("balanceLogListen EXIT");
	}

}
